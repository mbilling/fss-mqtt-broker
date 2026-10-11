/* `telemetry`: a worked example of a rule-function module for mqttd, in C
 * (ADR 0086 D9; docs/RULES.md, "Your own functions").
 *
 * It provides three functions to rule SQL:
 *
 *   crc16_modbus(Payload)   the Modbus RTU CRC-16 of the payload's bytes, an integer
 *   site_label(Text)        "<SITE>/<Text>": the SITE environment variable, which the
 *                           module sees only when its table lists it (env = ["SITE"])
 *                           and the broker's rules.functions_env does too; without the
 *                           grant the label is "/<Text>"
 *   clock_ms()              milliseconds since the epoch; 0 unless the table says
 *                           clock = true
 *
 * The function ABI, version 1 (all integers little-endian):
 *
 *   mqttd_abi_version() -> u32            1
 *   mqttd_alloc(len) -> address           memory the broker writes into
 *   mqttd_free(address)
 *   mqttd_describe(ret)                   JSON: module, version, functions
 *   mqttd_call(name, name_len, args, args_len, max_out, ret) -> status
 *
 * `ret` points at two u32: the address and the length of the result, which the broker
 * reads and then frees with mqttd_free. Status 0 is a value: one tag byte ('b' = a
 * string, as bytes; 'j' = any other value, as JSON text), then the bytes. Any other
 * status fails the rule, with the bytes as the message. `args` is a frame: a u32
 * count, then per argument a tag byte, a u32 length and the bytes.
 *
 * Built with `zig cc -target wasm32-wasi` (../build.sh). The C library's calls into
 * the host are sixteen WASI functions the sandbox provides, none of which reaches a
 * file or a socket; a module importing anything else does not load.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define EXPORT(name) __attribute__((export_name(name)))

enum { OK = 0, FAILED = 1, TOO_LARGE = 2, BAD_FRAME = 3 };

EXPORT("mqttd_abi_version") uint32_t mqttd_abi_version(void) { return 1; }

EXPORT("mqttd_alloc") void *mqttd_alloc(uint32_t len) { return malloc(len ? len : 1); }

EXPORT("mqttd_free") void mqttd_free(void *p) { free(p); }

/* Hands `len` bytes at `p` (malloc'ed, or NULL with 0) to the broker. */
static int32_t give(uint32_t *ret, int32_t status, char *p, size_t len) {
  ret[0] = (uint32_t)(uintptr_t)p;
  ret[1] = (uint32_t)len;
  return status;
}

/* A copy of the text `msg`, as the message of a failure. */
static int32_t fail(uint32_t *ret, int32_t status, const char *msg) {
  size_t n = strlen(msg);
  char *p = malloc(n ? n : 1);
  if (!p)
    return give(ret, status, NULL, 0);
  memcpy(p, msg, n);
  return give(ret, status, p, n);
}

EXPORT("mqttd_describe") void mqttd_describe(uint32_t *ret) {
  fail(ret, OK,
       "{\"module\":\"telemetry\",\"version\":\"0.1.0\",\"functions\":["
       "{\"name\":\"crc16_modbus\",\"min_args\":1,\"max_args\":1},"
       "{\"name\":\"site_label\",\"min_args\":1,\"max_args\":1},"
       "{\"name\":\"clock_ms\",\"min_args\":0,\"max_args\":0}]}");
}

/* One argument of the frame. */
typedef struct {
  char tag;
  const uint8_t *bytes;
  uint32_t len;
} arg;

static uint32_t u32_at(const uint8_t *p) {
  return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

/* Reads up to `max` arguments out of the frame; -1 when it is not a frame. */
static int arguments(const uint8_t *frame, uint32_t len, arg *out, int max) {
  if (len < 4)
    return -1;
  uint32_t count = u32_at(frame), at = 4;
  if (count > (uint32_t)max)
    return -1;
  for (uint32_t i = 0; i < count; i++) {
    if (len - at < 5)
      return -1;
    out[i].tag = (char)frame[at];
    out[i].len = u32_at(frame + at + 1);
    at += 5;
    if (len - at < out[i].len)
      return -1;
    out[i].bytes = frame + at;
    at += out[i].len;
  }
  return (int)count;
}

/* CRC-16/MODBUS: polynomial 0xA001 (reflected 0x8005), initial value 0xFFFF. */
static uint16_t crc16_modbus(const uint8_t *p, uint32_t len) {
  uint16_t crc = 0xFFFF;
  for (uint32_t i = 0; i < len; i++) {
    crc ^= p[i];
    for (int bit = 0; bit < 8; bit++)
      crc = (crc & 1) ? (uint16_t)((crc >> 1) ^ 0xA001) : (uint16_t)(crc >> 1);
  }
  return crc;
}

/* A value: the tag, then `len` bytes of `body`. */
static int32_t value(uint32_t *ret, uint32_t max_out, char tag, const char *body, size_t len) {
  if (len + 1 > max_out)
    return fail(ret, TOO_LARGE, "the result is larger than the caller allows");
  char *p = malloc(len + 1);
  if (!p)
    return fail(ret, FAILED, "out of memory");
  p[0] = tag;
  memcpy(p + 1, body, len);
  return give(ret, OK, p, len + 1);
}

static int is(const char *name, uint32_t name_len, const char *want) {
  return strlen(want) == name_len && memcmp(name, want, name_len) == 0;
}

EXPORT("mqttd_call")
int32_t mqttd_call(const char *name, uint32_t name_len, const uint8_t *frame, uint32_t frame_len,
                   uint32_t max_out, uint32_t *ret) {
  arg args[1];
  int count = arguments(frame, frame_len, args, 1);
  if (count < 0)
    return fail(ret, BAD_FRAME, "the arguments are not a frame");
  char text[32];

  if (is(name, name_len, "crc16_modbus") && count == 1) {
    if (args[0].tag != 'b')
      return fail(ret, FAILED, "crc16_modbus takes a string or a payload");
    int n = snprintf(text, sizeof text, "%u", (unsigned)crc16_modbus(args[0].bytes, args[0].len));
    return value(ret, max_out, 'j', text, (size_t)n);
  }

  if (is(name, name_len, "site_label") && count == 1) {
    if (args[0].tag != 'b')
      return fail(ret, FAILED, "site_label takes a string");
    const char *site = getenv("SITE");
    size_t site_len = site ? strlen(site) : 0, len = site_len + 1 + args[0].len;
    char *label = malloc(len);
    if (!label)
      return fail(ret, FAILED, "out of memory");
    if (site_len)
      memcpy(label, site, site_len);
    label[site_len] = '/';
    memcpy(label + site_len + 1, args[0].bytes, args[0].len);
    int32_t status = value(ret, max_out, 'b', label, len);
    free(label);
    return status;
  }

  if (is(name, name_len, "clock_ms") && count == 0) {
    struct timespec now = {0, 0};
    clock_gettime(CLOCK_REALTIME, &now);
    long long ms = (long long)now.tv_sec * 1000 + now.tv_nsec / 1000000;
    int n = snprintf(text, sizeof text, "%lld", ms);
    return value(ret, max_out, 'j', text, (size_t)n);
  }

  return fail(ret, FAILED, "no such function, or another number of arguments");
}
