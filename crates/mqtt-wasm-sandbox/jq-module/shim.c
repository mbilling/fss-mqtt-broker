/*
 * The jq rule function as a WebAssembly module (ADR 0086 spike).
 *
 * libjq behind the rule-function ABI, version 1:
 *
 *   mqttd_abi_version() -> u32
 *   mqttd_alloc(len) -> ptr            memory the host writes arguments into
 *   mqttd_free(ptr)
 *   mqttd_describe(ret)                JSON text: the functions this module provides
 *   mqttd_call(name, name_len, args, args_len, max_out, ret) -> status
 *   mqttd_cancel()                     optional: asks the running call to stop
 *
 * `ret` points at two u32: the address and the length of a buffer the host reads and
 * then frees with mqttd_free. `args` is a frame: u32 count, then per argument one tag
 * byte ('b' = the bytes of a binary, as they are; 'j' = any other value as JSON text),
 * a u32 length and the bytes. The result buffer is one value in the same form (tag
 * byte, then the bytes) when the status is 0, and a message when it is not.
 *
 * What this file does with libjq follows EMQX's jq NIF line by line
 * (emqx/jq v0.4.1, c_src/port_nif_common.c): the program and the input are C strings,
 * so both end at the first NUL; the input is read by jq's own parser; every output is
 * printed with jv_dump_string; the error texts and their tags are the NIF's. The
 * timeout is the NIF's too: when the deadline passes the host calls mqttd_cancel,
 * which is jq_cancel on the running program, and the program ends at its next step
 * with the instance and its compiled programs intact. A program that does not get to
 * a next step (inside one long regex match, say — which EMQX cannot stop at all) is
 * stopped by the host, and the instance with it.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "jq.h"
#include "jv.h"

#define EXPORT(name) __attribute__((export_name(name)))

/* The NIF's error tags, by number (c_src/port_nif_common.c `err_tags`). */
enum {
  ST_OK = 0,
  ST_SYSTEM = 2,
  ST_BADARG = 3,
  ST_COMPILE = 4,
  ST_PARSE = 5,
  ST_PROCESS = 6,
  ST_TIMEOUT = 7,
  ST_OUTPUT_TOO_LARGE = 8,
  ST_NO_SUCH_FUNCTION = 9,
  /* `jq` met a program this instance has not compiled: call `jq_compile` first. */
  ST_NOT_COMPILED = 10
};

/* JV_PRINT_SPACE1, the NIF's dump option. */
#define DUMPOPTS 512

/* Compiled programs kept between calls, least recently used out first. EMQX's NIF
 * keeps 500 per scheduler thread; an instance here serves one connection task. */
#ifndef PROGRAM_CACHE
#define PROGRAM_CACHE 32
#endif

typedef struct {
  char *program;
  jq_state *jq;
  uint64_t used;
} cached;

static cached cache[PROGRAM_CACHE];
/* The program inside jq_next, for mqttd_cancel. */
static jq_state *running;
static uint64_t clock_tick;
static uint32_t cache_hits, cache_misses;

/* A growing byte buffer. */
typedef struct {
  char *p;
  size_t len, cap;
} buf;

static int buf_put(buf *b, const char *s, size_t n) {
  if (b->len + n + 1 > b->cap) {
    size_t cap = b->cap ? b->cap * 2 : 256;
    while (cap < b->len + n + 1)
      cap *= 2;
    char *p = realloc(b->p, cap);
    if (!p)
      return 0;
    b->p = p;
    b->cap = cap;
  }
  memcpy(b->p + b->len, s, n);
  b->len += n;
  b->p[b->len] = 0;
  return 1;
}

/* The NIF's error callback: messages are appended to one another. */
static void err_cb(void *data, jv err) {
  buf *b = data;
  if (jv_get_kind(err) != JV_KIND_STRING)
    err = jv_dump_string(err, JV_PRINT_INVALID);
  const char *s = jv_string_value(err);
  buf_put(b, s, strlen(s));
  jv_free(err);
}

EXPORT("mqttd_abi_version") uint32_t mqttd_abi_version(void) { return 1; }

EXPORT("mqttd_alloc") void *mqttd_alloc(uint32_t len) { return malloc(len ? len : 1); }

EXPORT("mqttd_free") void mqttd_free(void *p) { free(p); }

/* Called by the host while mqttd_call is paused between two slices of fuel. Sets a
 * flag and nothing else, as a signal handler would. */
EXPORT("mqttd_cancel") void mqttd_cancel(void) {
  if (running)
    jq_cancel(running);
}

static void ret_bytes(uint32_t *ret, char *p, size_t len) {
  ret[0] = (uint32_t)(uintptr_t)p;
  ret[1] = (uint32_t)len;
}

static int32_t ret_message(uint32_t *ret, int32_t status, const char *msg) {
  size_t n = strlen(msg);
  char *p = malloc(n + 1);
  if (!p) {
    ret_bytes(ret, NULL, 0);
    return ST_SYSTEM;
  }
  memcpy(p, msg, n + 1);
  ret_bytes(ret, p, n);
  return status;
}

EXPORT("mqttd_describe") void mqttd_describe(uint32_t *ret) {
  ret_message(ret, 0,
              "{\"module\":\"jq\",\"version\":\"1.8.1+emqx.4c60b10\","
              "\"functions\":[{\"name\":\"jq\",\"min_args\":2,\"max_args\":2},"
              "{\"name\":\"jq_compile\",\"min_args\":1,\"max_args\":1}]}");
}

/* Cache statistics, for the spike's measurements: hits << 32 | misses. */
EXPORT("jq_cache_stats") uint64_t jq_cache_stats(void) {
  return ((uint64_t)cache_hits << 32) | cache_misses;
}

/* A compiled program for `program`, from the cache or (when `compile` is set)
 * compiled now. On failure the message is in `err` and NULL is returned. */
static jq_state *program_state(const char *program, int compile, buf *err, int32_t *status) {
  int victim = 0;
  for (int i = 0; i < PROGRAM_CACHE; i++) {
    if (cache[i].program && strcmp(cache[i].program, program) == 0) {
      cache[i].used = ++clock_tick;
      cache_hits++;
      return cache[i].jq;
    }
    if (cache[i].used < cache[victim].used)
      victim = i;
  }
  if (!compile) {
    *status = ST_NOT_COMPILED;
    return NULL;
  }
  cache_misses++;
  jq_state *jq = jq_init();
  if (!jq) {
    buf_put(err, "jq_init: Could not initialize jq", 32);
    *status = ST_SYSTEM;
    return NULL;
  }
  jq_set_error_cb(jq, err_cb, err);
  /* As the NIF: an empty library path, so `include` finds nothing. */
  jq_set_attr(jq, jv_string("JQ_LIBRARY_PATH"), jv_array());
  if (!jq_compile(jq, program)) {
    if (err->len == 0)
      buf_put(err, "Compilation of jq filter failed", 31);
    *status = ST_COMPILE;
    jq_teardown(&jq);
    return NULL;
  }
  err->len = 0;
  if (cache[victim].program) {
    free(cache[victim].program);
    jq_teardown(&cache[victim].jq);
  }
  cache[victim].program = strdup(program);
  cache[victim].jq = jq;
  cache[victim].used = ++clock_tick;
  return jq;
}

/* Reads one argument of the frame at *at; returns 0 when the frame is short. */
static int next_arg(const uint8_t **at, const uint8_t *end, uint8_t *tag, const char **p,
                    uint32_t *len) {
  if (end - *at < 5)
    return 0;
  *tag = (*at)[0];
  memcpy(len, *at + 1, 4);
  *at += 5;
  if ((size_t)(end - *at) < *len)
    return 0;
  *p = (const char *)*at;
  *at += *len;
  return 1;
}

/* A NUL-terminated copy: what the NIF hands libjq. */
static char *c_string(const char *p, uint32_t len) {
  char *s = malloc((size_t)len + 1);
  if (s) {
    memcpy(s, p, len);
    s[len] = 0;
  }
  return s;
}

static int32_t jq_eval(const char *program, const char *input, uint32_t max_out, uint32_t *ret) {
  buf err = {0};
  int32_t status = ST_OK;
  jq_state *jq = program_state(program, 0, &err, &status);
  if (!jq) {
    ret_bytes(ret, err.p, err.len);
    return status;
  }
  jq_set_error_cb(jq, err_cb, &err);
  jq_reset_cancel_state(jq);

  jv value = jv_parse_sized(input, (int)strlen(input));
  if (!jv_is_valid(value)) {
    if (err.len == 0) {
      value = jv_invalid_get_msg(value);
      const char *m = jv_string_value(value);
      buf_put(&err, m, strlen(m));
    }
    jv_free(value);
    ret_bytes(ret, err.p, err.len);
    return ST_PARSE;
  }

  /* The outputs as one JSON array text. */
  buf out = {0};
  buf_put(&out, "j[", 2);
  int first = 1, too_large = 0;
  jq_start(jq, value, 0);
  running = jq;
  jv result;
  while (jv_is_valid(result = jq_next(jq))) {
    jv text = jv_dump_string(result, DUMPOPTS);
    const char *s = jv_string_value(text);
    size_t n = strlen(s);
    if (out.len + n + 2 > (size_t)max_out) {
      too_large = 1;
      jv_free(text);
      break;
    }
    if (!first)
      buf_put(&out, ",", 1);
    first = 0;
    buf_put(&out, s, n);
    jv_free(text);
  }
  running = NULL;
  if (jq_canceled(jq)) {
    if (!too_large)
      jv_free(result);
    jq_start(jq, jv_null(), 0);
    free(out.p);
    free(err.p);
    return ret_message(ret, ST_TIMEOUT, "jq program canceled as it took too long time to execute");
  }
  if (too_large) {
    /* Leave the program runnable for the next call. */
    jq_start(jq, jv_null(), 0);
    free(out.p);
    free(err.p);
    return ret_message(ret, ST_OUTPUT_TOO_LARGE, "jq output is larger than the limit");
  }
  if (jv_invalid_has_msg(jv_copy(result))) {
    jv msg = jv_invalid_get_msg(jv_copy(result));
    buf m = {0};
    if (jv_get_kind(msg) == JV_KIND_STRING) {
      const char *s = jv_string_value(msg);
      buf_put(&m, "jq error: ", 10);
      /* %s in the NIF: the text ends at a NUL. */
      buf_put(&m, s, strlen(s));
    } else {
      msg = jv_dump_string(msg, 0);
      const char *s = jv_string_value(msg);
      buf_put(&m, "jq error (not a string): ", 25);
      buf_put(&m, s, strlen(s));
    }
    buf_put(&m, "\n", 1);
    jv_free(msg);
    jv_free(result);
    free(out.p);
    free(err.p);
    ret_bytes(ret, m.p, m.len);
    return ST_PROCESS;
  }
  jv_free(result);
  free(err.p);
  buf_put(&out, "]", 1);
  ret_bytes(ret, out.p, out.len);
  return ST_OK;
}

/* `jq_compile(program)`: compiles the program into the cache; the result is `true`.
 * Apart from `jq` because EMQX's timeout does not cover compilation (jq_cancel is
 * looked at only while a program runs): the host runs this under a fuel bound and
 * `jq` under the deadline. */
static int32_t jq_compile_only(const char *program, uint32_t *ret) {
  buf err = {0};
  int32_t status = ST_OK;
  if (!program_state(program, 1, &err, &status)) {
    ret_bytes(ret, err.p, err.len);
    return status;
  }
  free(err.p);
  return ret_message(ret, ST_OK, "jtrue");
}

EXPORT("mqttd_call")
int32_t mqttd_call(const char *name, uint32_t name_len, const uint8_t *args, uint32_t args_len,
                   uint32_t max_out, uint32_t *ret) {
  int compile_only = name_len == 10 && memcmp(name, "jq_compile", 10) == 0;
  if (!compile_only && (name_len != 2 || memcmp(name, "jq", 2) != 0))
    return ret_message(ret, ST_NO_SUCH_FUNCTION, "no such function");
  const uint8_t *at = args, *end = args + args_len;
  uint32_t count = 0;
  if (args_len < 4)
    return ret_message(ret, ST_BADARG, "short argument frame");
  memcpy(&count, at, 4);
  at += 4;
  uint8_t tag_p, tag_i;
  const char *p, *i;
  uint32_t p_len, i_len;
  if (compile_only) {
    if (count != 1 || !next_arg(&at, end, &tag_p, &p, &p_len) || tag_p != 'b')
      return ret_message(ret, ST_BADARG, "jq_compile takes a program (a binary)");
    char *text = c_string(p, p_len);
    if (!text)
      return ret_message(ret, ST_SYSTEM, "out of memory");
    int32_t status = jq_compile_only(text, ret);
    free(text);
    return status;
  }
  if (count != 2 || !next_arg(&at, end, &tag_p, &p, &p_len) ||
      !next_arg(&at, end, &tag_i, &i, &i_len) || tag_p != 'b')
    return ret_message(ret, ST_BADARG, "jq takes a program (a binary) and an input");
  char *program = c_string(p, p_len);
  char *input = c_string(i, i_len);
  if (!program || !input) {
    free(program);
    free(input);
    return ret_message(ret, ST_SYSTEM, "out of memory");
  }
  int32_t status = jq_eval(program, input, max_out, ret);
  free(program);
  free(input);
  return status;
}
