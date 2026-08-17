#!/usr/bin/env bash
# The converter's output must be accepted by the broker, not merely well-formed.
#
# A migration tool that emits plausible-looking TOML the broker then rejects is
# worse than none: it burns the evaluation it was meant to enable. So this feeds a
# realistic mosquitto.conf + acl_file through the converter and boots the real
# binary on the result.
set -euo pipefail
cd "$(dirname "$0")/../.."

MQTTD_BIN="${MQTTD_BIN:-target/debug/mqttd}"
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: $MQTTD_BIN not built"; exit 2; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/mosquitto.conf" <<'CONF'
persistence true
persistence_location /var/lib/mosquitto/
max_connections 5000
max_queued_messages 1000
allow_anonymous false
acl_file ACLPATH
password_file /etc/mosquitto/passwd
listener 1883 127.0.0.1
listener 8883 0.0.0.0
certfile /etc/certs/server.crt
keyfile /etc/certs/server.key
cafile /etc/certs/ca.crt
sys_interval 10
CONF
sed -i.bak "s|ACLPATH|$WORK/aclfile|" "$WORK/mosquitto.conf"

cat > "$WORK/aclfile" <<'ACL'
user sensor-1
topic write sensors/sensor-1/#
topic read commands/sensor-1/#
user admin
topic readwrite #
pattern read devices/%u/status
pattern write devices/%u/telemetry
ACL

python3 scripts/migrate/from-mosquitto.py "$WORK/mosquitto.conf" \
  --out-config "$WORK/mqttd.toml" --out-acl "$WORK/acl.toml" >/dev/null

[[ -s "$WORK/acl.toml" ]] || { echo "  FAIL — no ACL produced"; exit 1; }
# The converted CONFIG must be valid TOML. The fixture has two listeners on purpose:
# the converter once emitted [listeners] per listener, which tomllib rejects — and this
# harness never noticed, because the broker below is booted with env vars and the
# converted ACL only. Found by the 2026-08-11 review panel.
python3 - "$WORK/mqttd.toml" <<'PYEOF' || { echo "  FAIL — converted config is not valid TOML"; exit 1; }
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
echo "  ok   — converted config parses as TOML"

# THE assertion ADR 0051 §3's third rule demands, and which this harness lacked until
# 2026-08-14: parsing as TOML is not the same as being a config the broker ACCEPTS (a
# wrong type, an unknown key or a bad enum value all parse fine and then fail the load).
# The broker below is booted from env vars and the converted ACL only, so nothing else
# here puts the converted CONFIG in front of the real binary.
"$MQTTD_BIN" --check-config --config "$WORK/mqttd.toml" >/dev/null 2>"$WORK/check.err" \
  || { echo "  FAIL — the broker REJECTED the converted config:";
       sed 's/^/         /' "$WORK/check.err"; exit 1; }
echo "  ok   — the converted config passes 'mqttd --check-config'"

grep -q 'default = "deny"' "$WORK/acl.toml" || { echo "  FAIL — translated ACL is not deny-by-default"; exit 1; }
# %u must become mqttd's %i, or every pattern rule silently matches nothing.
grep -q '%i' "$WORK/acl.toml" || { echo "  FAIL — mosquitto's %u was not translated to %i"; exit 1; }
echo "  ok   — ACL is deny-by-default and %u became %i"

# ── hostile STRINGS: a Windows cert path and a domain-qualified username ──────────────
# The whole-class defect found on 2026-08-14 across all three converters: no value was
# escaped anywhere. `certfile C:\certs\server.crt` produced `cert = "C:\certs\server.crt"`
# and `user CORP\jdoe` produced `identities = ["CORP\jdoe"]`, neither of which is valid
# TOML — and a TOML parse failure is a WHOLE-DOCUMENT failure, so one such line made the
# broker refuse the entire migrated policy. Windows/AD estates are a normal input.
cat > "$WORK/hostile.conf" <<'CONF'
allow_anonymous false
acl_file HOSTILEACL
listener 8883 0.0.0.0
certfile C:\certs\server.crt
keyfile C:\certs\server.key
cafile C:\certs\ca.crt
require_certificate true
max_inflight_messages 20
CONF
sed -i.bak "s|HOSTILEACL|$WORK/hostile-aclfile|" "$WORK/hostile.conf"
cat > "$WORK/hostile-aclfile" <<'ACL'
user CORP\jdoe
topic readwrite sites/CORP\jdoe/#
ACL
python3 scripts/migrate/from-mosquitto.py "$WORK/hostile.conf" \
  --out-config "$WORK/hostile.toml" --out-acl "$WORK/hostile-acl.toml" >/dev/null
for f in hostile.toml hostile-acl.toml; do
  python3 - "$WORK/$f" <<'PYEOF' || { echo "  FAIL — $f is not valid TOML (an unescaped value)"; exit 1; }
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
done
"$MQTTD_BIN" --check-config --config "$WORK/hostile.toml" >/dev/null 2>"$WORK/hostile.err" \
  || { echo "  FAIL — the broker REJECTED the config built from hostile strings:";
       sed 's/^/         /' "$WORK/hostile.err"; exit 1; }
grep -q 'cert = "C:\\\\certs\\\\server.crt"' "$WORK/hostile.toml" \
  || { echo "  FAIL — the Windows certfile path was not TOML-escaped"; exit 1; }
grep -q 'client_ca = "C:\\\\certs\\\\ca.crt"' "$WORK/hostile.toml" \
  || { echo "  FAIL — require_certificate true + cafile did not become an escaped client_ca"; exit 1; }
grep -q 'identities = \["CORP\\\\jdoe"\]' "$WORK/hostile-acl.toml" \
  || { echo "  FAIL — the domain-qualified username was not TOML-escaped"; exit 1; }
echo "  ok   — Windows paths and a backslash username come out escaped and validate"

# max_inflight_messages is NOT [limits] receive_maximum: Mosquitto's bounds messages the
# BROKER may have in flight TOWARD a client (outbound), while receive_maximum is the MQTT 5
# Receive Maximum mqttd GRANTS clients — the inbound window. This table mapped one onto the
# other and the EMQX converter copied the error forward; found 2026-08-14.
if grep -qE '^receive_maximum = ' "$WORK/hostile.toml"; then
  echo "  FAIL — max_inflight_messages was mapped to receive_maximum (opposite directions)"; exit 1
fi
grep -q 'TODO(migrate): max_inflight_messages 20: NOT carried over' "$WORK/hostile.toml" \
  || { echo "  FAIL — max_inflight_messages was dropped without a TODO"; exit 1; }
echo "  ok   — max_inflight_messages is reported as a direction flip, not mapped"

# ── the translated ACL must actually be ENFORCED ──────────────────────────────────────
# Without [security] acl_file mqttd enforces NO authorization at all (mqtt-config:
# `acl_file: Option<String>`, None by default, "without it authorization is not enforced and
# loudly logged"). This converter translated a whole ACL policy and then never referenced it
# from the config it wrote, so the deployed broker authorized nothing while the generated
# ACL's own header said it denied by default. Found 2026-08-15 by sweeping the class.
grep -q '^acl_file = "/etc/mqttd/acl.toml"' "$WORK/mqttd.toml" \
  || { echo "  FAIL — the config does not name the translated ACL, so nothing enforces it"; exit 1; }
# ...and a config with NO acl_file must say authorization is off rather than stay silent.
printf 'persistence_location /v\nlistener 1883\n' > "$WORK/noacl.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/noacl.conf" --out-config "$WORK/noacl.toml" >/dev/null
if grep -qE '^acl_file = ' "$WORK/noacl.toml"; then
  echo "  FAIL — a config with no acl_file names an ACL that was never written"; exit 1
fi
grep -q 'NO authorization at all' "$WORK/noacl.toml" \
  || { echo "  FAIL — no acl_file, and the output does not say authorization is unenforced"; exit 1; }
echo "  ok   — the config names the translated ACL, and says so plainly when there is none"

# ── the fleet shape: an mTLS mandate on a listener that is NOT first ───────────────────
# THE defect round 2 proved on this converter, in the fail-open direction: `[tls]` was built
# from tls_listeners[0] and the gate read off first.tls["require_certificate"], so every later
# TLS listener's cafile / require_certificate / crlfile / capath vanished with no TODO naming
# them. `listener 1883` + `listener 8883 require_certificate true` + `listener 8884` is the
# textbook Mosquitto layout, and `mqttd --check-config` passes on the broken output.
cat > "$WORK/fleet.conf" <<'CONF'
persistence_location /var/lib/mosquitto
acl_file FLEETACL
listener 1883 127.0.0.1
listener 8884 0.0.0.0
certfile /certs/browser.crt
keyfile /certs/browser.key
tls_version tlsv1.2
listener 8883 0.0.0.0
certfile /certs/device.crt
keyfile /certs/device.key
cafile /certs/device-ca.crt
crlfile /certs/device.crl
capath /certs/extra-cas
require_certificate true
use_identity_as_username true
include_dir /etc/mosquitto/conf.d
CONF
sed -i.bak "s|FLEETACL|$WORK/aclfile|" "$WORK/fleet.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/fleet.conf" \
  --out-config "$WORK/fleet.toml" --out-acl "$WORK/fleet-acl.toml" >/dev/null
"$MQTTD_BIN" --check-config --config "$WORK/fleet.toml" >/dev/null 2>"$WORK/fleet.err" \
  || { echo "  FAIL — the broker REJECTED the fleet-shaped config:";
       sed 's/^/         /' "$WORK/fleet.err"; exit 1; }
for needle in \
  'TLS listeners DISAGREE about client certificates' \
  '/certs/device-ca.crt' \
  '/certs/device.crl' \
  '/certs/extra-cas' \
  'tls_version tlsv1.2' \
  'use_identity_as_username true' \
  '/etc/mosquitto/conf.d'; do
  grep -qF "$needle" "$WORK/fleet.toml" \
    || { echo "  FAIL — the non-first TLS listener's '$needle' left no trace"; exit 1; }
done
# The posture is MIXED (the browser listener never required a certificate), so an ACTIVE
# client_ca would silently demand certificates from every wss/QUIC client, and an active crl
# beside it is a config the broker refuses outright.
if grep -qE '^client_ca = ' "$WORK/fleet.toml"; then
  echo "  FAIL — a MIXED mTLS posture was mapped to client_ca"; exit 1
fi
if grep -qE '^crl = ' "$WORK/fleet.toml"; then
  echo "  FAIL — crl was emitted without an active client_ca (the broker refuses that pair)"; exit 1
fi
grep -q '# client_ca = "/certs/device-ca.crt"' "$WORK/fleet.toml" \
  || { echo "  FAIL — no commented client_ca candidate for the mixed posture"; exit 1; }
grep -q 'mtls_identity_source = "cn"' "$WORK/fleet.toml" \
  || { echo "  FAIL — use_identity_as_username true did not become mtls_identity_source"; exit 1; }
echo "  ok   — every TLS listener is read; a mixed mandate is refused, not guessed either way"

# ── TLS-PSK: an ENCRYPTED listener mqttd cannot express ────────────────────────────────
# THE round-4 blocking defect. psk_file/psk_hint were in neither TLS_KEYS nor the
# half-material safety net, so `is_tls` was false and BIND_KEYS[(transport, False)] picked
# `plaintext_bind`: a listener Mosquitto served over TLS-PSK became a LIVE PLAINTEXT bind, on
# the same port, while another TODO in the same file said that listener spoke TLS 1.2. The
# provenance gate cannot catch it — the bind carried a genuine `# from: listener 8883` — because
# the gate checks where the VALUE came from and the FIELD is what encodes the transport.
cat > "$WORK/psk.conf" <<'CONF'
persistence_location /v
listener 8883
psk_file /etc/mosq/psk
psk_hint pskid
tls_version tlsv1.2
CONF
python3 scripts/migrate/from-mosquitto.py "$WORK/psk.conf" --out-config "$WORK/psk.toml" >/dev/null
"$MQTTD_BIN" --check-config --config "$WORK/psk.toml" >/dev/null 2>&1 \
  || { echo "  FAIL — the broker REJECTED the config built from a PSK listener"; exit 1; }
if grep -qE '^(plaintext|ws)_bind = ' "$WORK/psk.toml"; then
  echo "  FAIL — a TLS-PSK listener became a LIVE PLAINTEXT bind (an encrypted transport downgraded to cleartext)"; exit 1
fi
grep -q '# tls_bind = "0.0.0.0:8883"' "$WORK/psk.toml" \
  || { echo "  FAIL — the PSK listener's bind is not emitted as an INERT candidate on the TLS key"; exit 1; }
for needle in 'ENCRYPTED WITH TLS-PSK' 'NO PSK SUPPORT AT ALL' 'DOWNGRADE an encrypted transport' '/etc/mosq/psk'; do
  grep -qF "$needle" "$WORK/psk.toml" \
    || { echo "  FAIL — the PSK listener's '$needle' is not reported"; exit 1; }
done
echo "  ok   — a TLS-PSK listener is inert and named, never a plaintext bind"

# ── an address the BROKER cannot bind ──────────────────────────────────────────────────
# `mqttd --check-config` accepts ANY string in a bind (resolution happens at bind time), so the
# verification this converter's header, --help and docs point the operator at verified NOTHING
# about the one value the whole provenance restructuring is about. `listener 0 /tmp/mosq.sock`
# (mosquitto.conf(5): "the port must be set to 0, and the unix socket path must be given")
# declares no TCP endpoint at all, and produced a live `plaintext_bind = "/tmp/mosq.sock:0"`.
printf 'persistence_location /v\nlistener 0 /tmp/mosq.sock\n' > "$WORK/sock.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/sock.conf" --out-config "$WORK/sock.toml" >/dev/null
if grep -qE '^plaintext_bind = ' "$WORK/sock.toml"; then
  echo "  FAIL — a UNIX-socket listener became a live TCP bind the broker cannot bind"; exit 1
fi
grep -qF 'unix-socket transport' "$WORK/sock.toml" \
  || { echo "  FAIL — the unix-socket listener is not reported as unmappable"; exit 1; }
echo "  ok   — a bind mqttd cannot bind comes out inert, not merely --check-config clean"

# ── the ANONYMOUS-scoped ACL block, and two constructs mqttd cannot express ────────────
# mosquitto.conf(5) @ v2.0.22, verbatim: "The first set of topics are applied to anonymous
# clients, assuming allow_anonymous is true". Those pre-`user` lines were emitted with NO
# identities, which mqttd applies to EVERY authenticated client — strictly broader than the
# source under both postures, on the artifact docs/MIGRATION.md calls the dangerous half.
cat > "$WORK/anon-acl" <<'ACL'
topic read public/#
topic readwrite anon/#
user alice
topic readwrite private/alice/#
user alice*bob
topic write out/#
user bob
topic read c/%c/x
ACL
printf 'persistence_location /v\nlistener 1883\nallow_anonymous false\nacl_file %s\n' \
  "$WORK/anon-acl" > "$WORK/anon.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/anon.conf" \
  --out-config "$WORK/anon.toml" --out-acl "$WORK/anon-acl.toml" >/dev/null
if grep -q 'no identities = applies to every authenticated client' "$WORK/anon-acl.toml"; then
  echo "  FAIL — an ANONYMOUS-only grant was widened to every authenticated identity"; exit 1
fi
grep -q 'identities = \["anonymous"\]' "$WORK/anon-acl.toml" \
  || { echo "  FAIL — the anonymous-scoped block was not scoped to mqttd's anonymous subject"; exit 1; }
grep -qF 'applied to anonymous clients' "$WORK/anon-acl.toml" \
  || { echo "  FAIL — the anonymous scope is not named, quoting the man page"; exit 1; }
# ...and mqttd has NO escape for either metacharacter, so both rules must be refused, not widened.
if grep -q 'identities = \["alice\*bob"\]' "$WORK/anon-acl.toml"; then
  echo "  FAIL — a LITERAL '*' in a username became an mqttd identity GLOB"; exit 1
fi
if grep -q '"c/%c/x"' "$WORK/anon-acl.toml"; then
  echo "  FAIL — a literal 'topic' filter with %c became a SUBSTITUTING mqttd rule"; exit 1
fi
grep -qF 'alice*bob' "$WORK/anon-acl.toml" \
  || { echo "  FAIL — the refused username is not named anywhere"; exit 1; }
grep -qF 'c/%c/x' "$WORK/anon-acl.toml" \
  || { echo "  FAIL — the refused literal filter is not named anywhere"; exit 1; }
echo "  ok   — an anonymous block is scoped, and constructs mqttd cannot express are refused"

# ── the anonymous-scope TODO must name a line that EXISTS in the output ────────────────
# Its remediation advice said those rules "grant NOTHING until [security] allow_anonymous is
# set in the generated config, and it is emitted COMMENTED OUT" — but the converter only
# writes that commented candidate when mosquitto.conf set allow_anonymous TRUE. The fixture
# above says `allow_anonymous false`, so there is no such line anywhere in the output and the
# reader is sent to uncomment something that was never written. Advice that names a line the
# converter does not emit is how an operator concludes the tool is lying about the rest.
if grep -qE '^# allow_anonymous = ' "$WORK/anon.toml"; then
  echo "  FAIL — the allow_anonymous-false fixture unexpectedly carries a commented allow_anonymous candidate; this case no longer tests what it says"; exit 1
fi
if grep -qF '`# allow_anonymous = true`' "$WORK/anon-acl.toml"; then
  echo "  FAIL — the anonymous-scope TODO sends the reader to uncomment a commented allow_anonymous line that is NOT in the generated config (mosquitto.conf said allow_anonymous false, so the converter wrote no such line)"; exit 1
fi
grep -qF 'NO allow_anonymous line at all' "$WORK/anon-acl.toml" \
  || { echo "  FAIL — with allow_anonymous false, the anonymous-scope TODO does not say the generated config contains no allow_anonymous line"; exit 1; }
# ...and where Mosquitto DID allow anonymous clients, the advice must quote the line that IS
# written, verbatim, so the operator can find it by grep.
printf 'persistence_location /v\nlistener 1883\nallow_anonymous true\nacl_file %s\n' \
  "$WORK/anon-acl" > "$WORK/anontrue.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/anontrue.conf" \
  --out-config "$WORK/anontrue.toml" --out-acl "$WORK/anontrue-acl.toml" >/dev/null
grep -qF '# allow_anonymous = true' "$WORK/anontrue.toml" \
  || { echo "  FAIL — allow_anonymous true left no commented candidate in the config"; exit 1; }
grep -qF '`# allow_anonymous = true`' "$WORK/anontrue-acl.toml" \
  || { echo "  FAIL — the anonymous-scope TODO does not quote the commented allow_anonymous line the generated config actually holds"; exit 1; }
echo "  ok   — the anonymous-scope TODO names the allow_anonymous line the output really has, in both postures"

# ── the vendor's sentinels and the vendor's own words ──────────────────────────────────
# `message_size_limit 0` is mosquitto.conf(5)'s spelling of NO LIMIT ("The default value is 0,
# which means that all valid MQTT messages are accepted"), and mqttd FLOORS max_packet_size to
# 1024 — so passing the 0 through turned an unlimited broker into one refusing packets over
# 1 KiB, with `config OK` from --check-config. And the NOTE beside a real value made two claims
# the pinned man page contradicts: it does NOT deprecate message_size_limit (it marks port,
# bind_address, allow_duplicate_messages and clientid_prefixes deprecated, not this one), and
# the two are NOT the same quantity — message_size_limit is the "maximum publish payload size"
# while max_packet_size "applies to the full MQTT packet, not just the payload".
printf 'persistence_location /v\nlistener 1883\nmessage_size_limit 0\n' > "$WORK/zero.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/zero.conf" --out-config "$WORK/zero.toml" >/dev/null
if grep -qE '^max_packet_size = 0$' "$WORK/zero.toml"; then
  echo "  FAIL — the vendor's UNLIMITED sentinel became a 1 KiB packet ceiling"; exit 1
fi
grep -qF 'documents as NO LIMIT' "$WORK/zero.toml" \
  || { echo "  FAIL — the 0 sentinel was dropped without a NOTE"; exit 1; }
printf 'persistence_location /v\nlistener 1883\nmessage_size_limit 65536\n' > "$WORK/msl.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/msl.conf" --out-config "$WORK/msl.toml" >/dev/null
grep -qE '^max_packet_size = 65536' "$WORK/msl.toml" \
  || { echo "  FAIL — message_size_limit did not map to max_packet_size"; exit 1; }
if grep -qF 'deprecates message_size_limit' "$WORK/msl.toml"; then
  echo "  FAIL — the NOTE still claims mosquitto.conf(5) deprecates message_size_limit (it does not)"; exit 1
fi
if grep -qF 'which is the same quantity' "$WORK/msl.toml"; then
  echo "  FAIL — the NOTE still claims payload size and packet size are the same quantity"; exit 1
fi
grep -qF 'not just the payload' "$WORK/msl.toml" \
  || { echo "  FAIL — the NOTE does not state the payload-vs-packet difference"; exit 1; }
echo "  ok   — the packet-size sentinel and the payload/packet caveat match the man page"

# ── a value the VENDOR'S OWN SCHEMA does not admit: refused, never emitted ─────────────
# The DIRECT table's `int` arm ended in conv.set() with NO check that the value was a number,
# so a typo'd `message_size_limit 10OO` (a letter O) wrote `max_packet_size = 10OO` into the
# generated TOML: a document `mqttd --check-config` cannot even PARSE, so the validation this
# converter's own output points at could not run — and the converter had already exited 0,
# which a migration script reads as success. The bool arm flipped the other way: truthy()
# reads anything it does not recognise as FALSE, so `retain_available flase` was taken as TRUE
# and neither translated nor reported.
cat > "$WORK/malformed.conf" <<'CONF'
persistence_location /v
listener 1883
message_size_limit 10OO
max_queued_messages abc
retain_available flase
CONF
set +e
python3 scripts/migrate/from-mosquitto.py "$WORK/malformed.conf" \
  --out-config "$WORK/malformed.toml" >"$WORK/malformed.out" 2>"$WORK/malformed.err"
malformed_rc=$?
set -e
if [[ $malformed_rc -eq 0 ]]; then
  echo "  FAIL — a mosquitto.conf carrying values the vendor's schema forbids was 'translated' and the tool exited 0, which a migration script reads as success:"
  grep -nE '^(max_packet_size|max_queued_messages|max_retained_messages) ' "$WORK/malformed.toml" | sed 's/^/         /'
  exit 1
fi
[[ $malformed_rc -eq 1 ]] || { echo "  FAIL — the refusal exited $malformed_rc; the documented contract is 0 translated / 1 unusable input"; exit 1; }
if [[ -e "$WORK/malformed.toml" ]]; then
  echo "  FAIL — a config file was written from a malformed source; nothing should be written at all"; exit 1
fi
for needle in 'message_size_limit 10OO' 'max_queued_messages abc' 'retain_available flase' "$WORK/malformed.conf"; do
  grep -qF "$needle" "$WORK/malformed.err" \
    || { echo "  FAIL — the refusal does not name '$needle', so the operator cannot tell which key, value or file to fix"; exit 1; }
done
# A count that IS a number must still be translated — the refusal must not swallow valid input.
printf 'persistence_location /v\nlistener 1883\nmax_queued_messages 1000\nretain_available false\n' > "$WORK/wellformed.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/wellformed.conf" --out-config "$WORK/wellformed.toml" >/dev/null
grep -qE '^max_queued_messages = 1000$' "$WORK/wellformed.toml" \
  || { echo "  FAIL — a well-formed count stopped being translated"; exit 1; }
echo "  ok   — a count that is not a number and a boolean that is not a boolean are refused with nothing written"

# ── TLS KNOBS with NO certificate: plaintext, and the note must SAY plaintext ──────────
# A listener carrying tls_version / ciphers / dhparamfile but no certfile is NOT a TLS
# listener — Mosquitto needs a certificate to terminate TLS — so those knobs were INERT. The
# converter emitted a live PLAINTEXT bind for it (right: it WAS plaintext) beside a TODO
# saying that listener "accepted TLS 1.2 AND 1.3", which an operator reads as "encrypted
# before cutover, not after". That is the dangerous direction of the same misreading class as
# the PSK listener above.
cat > "$WORK/knobs.conf" <<'CONF'
persistence_location /v
listener 8883 0.0.0.0
tls_version tlsv1.2
ciphers ECDHE-RSA-AES128-GCM-SHA256
dhparamfile /etc/mosq/dh.pem
CONF
python3 scripts/migrate/from-mosquitto.py "$WORK/knobs.conf" --out-config "$WORK/knobs.toml" >/dev/null
"$MQTTD_BIN" --check-config --config "$WORK/knobs.toml" >/dev/null 2>&1 \
  || { echo "  FAIL — the broker REJECTED the config built from a certificate-less TLS-knob listener"; exit 1; }
grep -q '^plaintext_bind = "0.0.0.0:8883"' "$WORK/knobs.toml" \
  || { echo "  FAIL — the certificate-less listener lost its plaintext bind; it WAS plaintext and the output must say so"; exit 1; }
if grep -qF 'accepted TLS 1.2 AND 1.3' "$WORK/knobs.toml"; then
  echo "  FAIL — the output asserts a listener with NO certfile terminated TLS ('accepted TLS 1.2 AND 1.3') beside a live PLAINTEXT bind; the operator concludes traffic was encrypted before cutover and is not after"; exit 1
fi
for needle in 'did NOT terminate TLS' 'was INERT' 'ciphers ECDHE-RSA-AES128-GCM-SHA256' 'dhparamfile /etc/mosq/dh.pem' 'tls_version tlsv1.2'; do
  grep -qF "$needle" "$WORK/knobs.toml" \
    || { echo "  FAIL — the certificate-less listener's output does not say '$needle'"; exit 1; }
done
# ...and a listener that really DOES terminate TLS must keep its version-floor translation,
# and its cipher list must be reported rather than dropped.
printf 'persistence_location /v\nlistener 8883 0.0.0.0\ncertfile /c.crt\nkeyfile /c.key\ntls_version tlsv1.2\nciphers HIGH\ndhparamfile /dh.pem\n' > "$WORK/realtls.conf"
python3 scripts/migrate/from-mosquitto.py "$WORK/realtls.conf" --out-config "$WORK/realtls.toml" >/dev/null
grep -qF 'accepted TLS 1.2 AND 1.3' "$WORK/realtls.toml" \
  || { echo "  FAIL — a listener that really terminates TLS lost its tls_version floor report"; exit 1; }
for needle in 'ciphers HIGH' 'dhparamfile /dh.pem'; do
  grep -qF "$needle" "$WORK/realtls.toml" \
    || { echo "  FAIL — a real TLS listener's '$needle' is dropped without a word"; exit 1; }
done
echo "  ok   — inert TLS knobs are reported as inert, and only a listener with a certificate is said to have terminated TLS"

# ── a BRIDGE block: reported, and pointed at the document that has the answer ──────────
# Every one of these has an exact equivalent in the mqtt-bridge config this repo ships, and all
# but `connection` used to be reported as "no direct equivalent — check the mqttd configuration
# table", which has nothing to find. `bridge_cafile` decides whether the migrated bridge
# verifies its peer.
cat > "$WORK/bridge.conf" <<'CONF'
persistence_location /v
listener 1883
connection remote-site
address up.example.com:8883
topic fleet/# both 0 "" ""
bridge_cafile /certs/CA.crt
remote_username U
CONF
python3 scripts/migrate/from-mosquitto.py "$WORK/bridge.conf" --out-config "$WORK/bridge.toml" >/dev/null
for needle in \
  'address up.example.com:8883' \
  'bridge_cafile /certs/CA.crt' \
  'remote_username U' \
  'topic fleet/# both'; do
  grep -qF "$needle" "$WORK/bridge.toml" \
    || { echo "  FAIL — the bridge directive '$needle' is not reported"; exit 1; }
done
for target in \
  "mqtt-bridge \`[[upstreams]] url\`" \
  "mqtt-bridge \`[upstreams.tls] ca\`" \
  "mqtt-bridge \`[[upstreams]] username\`" \
  "mqtt-bridge \`[[upstreams.rules]]\`"; do
  grep -qF "$target" "$WORK/bridge.toml" \
    || { echo "  FAIL — a bridge directive is reported without naming its equivalent ($target)"; exit 1; }
done
if grep -qF 'bridge_cafile /certs/CA.crt: no direct equivalent' "$WORK/bridge.toml"; then
  echo "  FAIL — bridge_cafile is still filed under 'no equivalent'"; exit 1
fi
echo "  ok   — a bridge block names its mqtt-bridge equivalents, not a table with nothing in it"

# ── the --help epilog must not claim a verification this converter never had ───────────
# It printed "VERIFIED: fixtures diffed against pinned vendor sources" twenty lines under its
# own PROVENANCE paragraph saying NO vendor file is pinned for this converter.
HELP="$(python3 scripts/migrate/from-mosquitto.py --help)"
if printf '%s' "$HELP" | grep -qF 'fixtures diffed against pinned vendor sources'; then
  echo "  FAIL — --help claims vendor-diffed fixtures for a converter that has none"; exit 1
fi
printf '%s' "$HELP" | grep -qF 'NOT diffed against vendor bytes' \
  || { echo "  FAIL — --help does not say it has no pinned vendor fixture"; exit 1; }
printf '%s' "$HELP" | grep -qF 'MEANING it misreads' \
  || { echo "  FAIL — --help does not disclose the semantic-misreading class the gate cannot catch"; exit 1; }
echo "  ok   — --help claims exactly what this converter has"

# THE assertion: the real broker accepts it. `mqttd --check-config` does NOT read the file
# [security] acl_file names (verified: a policy with `default = "bogus"` still reports config OK),
# so BOOTING the broker is the only check that the translated policy loads — and it is run on the
# anonymous-scoped policy too, because `identities = ["anonymous"]` is a construct this converter
# only started emitting on 2026-08-15.
boot_on_acl() {
  local acl="$1" what="$2" port hport
  port=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
  hport=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
  MQTTD_ACL_FILE="$acl" MQTTD_PLAINTEXT_BIND="127.0.0.1:$port" \
    MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
    MQTTD_ALLOW_ANONYMOUS=1 MQTTD_HEALTH_BIND="127.0.0.1:$hport" RUST_LOG=warn \
    "$MQTTD_BIN" > "$WORK/boot.log" 2>&1 &
  local broker=$!
  for _ in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$hport/readyz" >/dev/null 2>&1 && break
    sleep 0.1
  done
  if ! curl -fsS "http://127.0.0.1:$hport/readyz" >/dev/null 2>&1; then
    echo "  FAIL — the broker REJECTED $what:"
    tail -5 "$WORK/boot.log" | sed 's/^/         /'
    kill "$broker" 2>/dev/null || true
    exit 1
  fi
  kill "$broker" 2>/dev/null || true
  wait "$broker" 2>/dev/null || true   # reap it quietly; an unreaped job prints "Terminated"
  echo "  ok   — the broker booted on $what"
}
boot_on_acl "$WORK/acl.toml" "the converted ACL"
boot_on_acl "$WORK/anon-acl.toml" "the ACL holding the anonymous-scoped rules"
# ── the PROPERTY SWEEP ──────────────────────────────────────────────────────────────────
# Everything above is example-based: one input, a list of greps. That shape catches a
# regression exactly where a reviewer already looked and is blind everywhere else — which is
# how the same first-listener-only TLS defect was found three times in three converters, each
# harness having only ever fed its converter ONE ordering of ONE listener set. The sweep
# generates many inputs (listener ORDER, enable flags, mTLS postures, no_match postures,
# truststore presence) and asserts one invariant per defect CLASS on every one of them:
# nothing silently dropped, nothing disabled activated, no claim contradicting the value it
# describes, no dangling `step N`, and `--check-config` on every generated config.
python3 scripts/migrate/property_sweep.py mosquitto --mqttd "$MQTTD_BIN" \
  || { echo "  FAIL — the mosquitto property sweep found a case this harness cannot see";
       exit 1; }

# ── the FUZZ pass ───────────────────────────────────────────────────────────────────────
# The generators above enumerate axes their author thought of, which is how round 2's
# blocking defect survived round 1. This pass does not think: it mutates each fixture
# mechanically (delete lines, truncate mid-structure, permute blocks, flip enable flags, swap
# transports) and asserts only what must hold for ANY byte sequence — the converter EXITS,
# 0 or 1, with a message; whatever it writes is valid TOML; and no live security-relevant
# line lacks provenance. It is how the HOCON reader's infinite loop was found (a two-line
# input: `authentication = [` then `}`), which no example-based test could have produced.
python3 scripts/migrate/property_sweep.py mosquitto --fuzz 40 \
  || { echo "  FAIL — the mosquitto fuzz pass found an input this converter wedges, crashes or invents on";
       exit 1; }

echo "MIGRATE OK"
