#!/usr/bin/env bash
# The EMQX converter's output must be accepted by the broker, not merely well-formed.
#
# A migration tool that emits plausible-looking TOML the broker then rejects is worse than
# none: it burns the evaluation it was meant to enable. So this feeds the vendor-derived
# fixtures through the converter and then asks the REAL binaries to accept the results —
# `mqttd --check-config` on the config (ADR 0051 §3's third rule, which the Mosquitto
# harness never actually enforced), a real broker BOOT on the ACL, and `mqtt-bridge`'s own
# parse+validate on the bridge config.
#
# It also asserts the contract's other half, which is the whole reason the tool is trusted:
# untranslatable input comes out as a TODO(migrate) line, never a silent drop and never a
# crash — proven on an adversarial fixture (unknown keys, whole subsystems with no
# equivalent, malformed input), an empty file, and the vendor's own default ACL whose four
# rules each land on a different gap in mqttd's ACL model.
set -euo pipefail
cd "$(dirname "$0")/../.."

MQTTD_BIN="${MQTTD_BIN:-target/debug/mqttd}"
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: $MQTTD_BIN not built"; exit 2; }
BRIDGE_BIN="${MQTT_BRIDGE_BIN:-target/debug/mqtt-bridge}"
[[ -x "$BRIDGE_BIN" ]] || { echo "FATAL: $BRIDGE_BIN not built"; exit 2; }

FIX=scripts/migrate/fixtures
CONV=scripts/migrate/from-emqx.py

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

ok()   { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }

# ── 1. the vendor fixture converts ───────────────────────────────────────────────────
python3 "$CONV" "$FIX/emqx-6.2.2.conf" \
  --acl-file "$FIX/emqx-acl-documented-examples.conf" \
  --out-config "$WORK/mqttd.toml" --out-acl "$WORK/acl.toml" \
  --out-bridge "$WORK/bridge.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the vendor fixture"
[[ -s "$WORK/mqttd.toml" && -s "$WORK/acl.toml" && -s "$WORK/bridge.toml" ]] \
  || fail "the converter produced an empty output file"
ok "the vendor fixture (emqx/emqx @ 6.2.2 examples) converts"

# ── 2. the converted documents are valid TOML ────────────────────────────────────────
# A TOML table may be declared only once, and EMQX names listeners freely — so several
# listeners of one protocol is the shape that once produced output tomllib rejects in the
# Mosquitto converter (#162). The fixture has one per protocol; the adversarial one has three.
for f in mqttd.toml acl.toml bridge.toml; do
  python3 - "$WORK/$f" <<'PYEOF' || fail "$f is not valid TOML"
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
done
ok "the converted config, ACL and bridge config all parse as TOML"

# ── 3. THE assertion ADR 0051 §3 demands: the config passes --check-config ────────────
"$MQTTD_BIN" --check-config --config "$WORK/mqttd.toml" >/dev/null 2>"$WORK/check.err" \
  || { echo "  FAIL — the broker REJECTED the converted config:";
       sed 's/^/         /' "$WORK/check.err"; exit 1; }
ok "the converted config passes 'mqttd --check-config'"

# ── 4. the ACL kept its posture and translated its substitutions ──────────────────────
grep -q 'default = "deny"' "$WORK/acl.toml" || fail "the translated ACL is not deny-by-default"
# ${clientid} -> %c and ${username} -> %i, or every templated rule silently matches nothing.
grep -q '"%c/#"' "$WORK/acl.toml" || fail "EMQX \${clientid} was not translated to %c"
grep -q 'devices/%i/telemetry' "$WORK/acl.toml" || fail "EMQX \${username} was not translated to %i"
ok "the ACL is deny-by-default and \${clientid}/\${username} became %c/%i"

# ── 5. the mappings that must be present ─────────────────────────────────────────────
# Each of these is a rule that translates; deleting the rule must break this test (the
# mutation proof recorded in docs/delivery/0051-evaluation-readiness.md).
grep -q 'max_packet_size = 1048576' "$WORK/mqttd.toml" || fail "mqtt.max_packet_size (1MB) was not normalised to bytes"
grep -q 'max_queued_messages = 1000' "$WORK/mqttd.toml"|| fail "mqtt.max_mqueue_len did not become max_queued_messages"
grep -q 'max_publish_rate = 1000' "$WORK/mqttd.toml"   || fail "listener messages_rate=\"1000/s\" did not become max_publish_rate"
grep -q 'max_retained_messages = 100000' "$WORK/mqttd.toml" || fail "retainer max_retained_messages was not carried over"
grep -q 'plaintext_bind = "0.0.0.0:1883"' "$WORK/mqttd.toml" || fail "the tcp listener's bare port did not become plaintext_bind"
grep -q 'tls_bind = "0.0.0.0:8883"' "$WORK/mqttd.toml" || fail "the ssl listener did not become tls_bind"
grep -q 'quic_bind = "0.0.0.0:14567"' "$WORK/mqttd.toml" || fail "the quic listener did not become quic_bind"
grep -q 'mtls_identity_source = "cn"' "$WORK/mqttd.toml" || fail "peer_cert_as_username=cn did not become mtls_identity_source"
# Per-listener connection caps collapse onto ONE node-wide cap, and the collapse must take
# the SMALLER of them and say so — taking the larger would silently RAISE a limit the
# operator set. The fixture ships 1000000 (tcp) against 500000 (ssl) for exactly this.
grep -q 'max_connections = 500000' "$WORK/mqttd.toml" \
  || fail "two listener max_connections (1000000, 500000) did not collapse onto the SMALLEST"
grep -q 'TODO(migrate): several listeners set DIFFERENT max_connections' "$WORK/mqttd.toml" \
  || fail "the max_connections collapse happened silently — no TODO naming the values"
ok "listeners, limits, the smallest-wins connection cap and the mTLS identity source all mapped"

# ── 5b. mqtt.max_inflight is NOT receive_maximum, and must not be presented as one ─────
# They are OPPOSITE DIRECTIONS: EMQX's max_inflight bounds messages the BROKER may have in
# flight TOWARD a client, while [limits] receive_maximum is the MQTT 5 Receive Maximum
# mqttd GRANTS clients — the inbound window (crates/mqtt-config/src/lib.rs). The fixture
# ships EMQX's own default of 32 against mqttd's 256, so the old mapping cut every stock
# conversion's inbound window 8x. Found 2026-08-14.
if grep -qE '^receive_maximum = ' "$WORK/mqttd.toml"; then
  fail "mqtt.max_inflight was mapped to [limits] receive_maximum — opposite directions"
fi
grep -q 'TODO(migrate): mqtt.max_inflight = 32 was NOT mapped' "$WORK/mqttd.toml" \
  || fail "mqtt.max_inflight was dropped without a TODO explaining the direction flip"
ok "mqtt.max_inflight is reported as a direction flip, not mapped onto receive_maximum"

# ── 6. the TODO(migrate) markers that must be present ────────────────────────────────
# The issue's acceptance criterion: "expected output + TODO markers out".
todo() { grep -q "TODO(migrate).*$1" "$2" || fail "no TODO(migrate) for: $1"; }
todo 'rule_engine'                "$WORK/mqttd.toml"   # no equivalent AT ALL
todo 'dashboard'                  "$WORK/mqttd.toml"   # absent by design
todo 'exhook'                     "$WORK/mqttd.toml"   # no hook API
todo 'hash-password'              "$WORK/mqttd.toml"   # built-in users: re-enrol, not convert
todo 'no mysql authentication backend' "$WORK/mqttd.toml"
todo 'built_in_database.*CANNOT SEE IT'  "$WORK/mqttd.toml"  # the invisible ACL source
todo 'cluster'                    "$WORK/mqttd.toml"   # topology is not translated
todo 'OCSP'                       "$WORK/mqttd.toml"
todo 'TLS 1.2'                    "$WORK/mqttd.toml"
todo 'EVALUATION ORDER CHANGED'   "$WORK/acl.toml"     # first-match-wins vs deny-wins
todo 'qos/retain flags'           "$WORK/acl.toml"     # the qualifier mqttd cannot express
ok "every expected TODO(migrate) marker is present"

# ── 6b. EVERY TLS listener is reported, and a mixed mTLS posture is never guessed ───────
# The fixture has an ssl listener that MANDATES client certificates (verify_peer +
# fail_if_no_peer_cert) and a quic listener with verify = verify_none. mqttd has ONE [tls]
# table which it applies to tls_bind, wss_bind AND quic_bind (one shared acceptor plus
# quic::server_endpoint, crates/mqttd/src/main.rs), so neither arm is a translation:
# mapping client_ca newly mandates mTLS on QUIC, and dropping it loses the ssl listener's
# mandate. Round 1 found the converter reading listener[0] only, so the quic listener was
# mentioned NOWHERE and the one warning claimed the others' material "was NOT carried
# over" — the inverse of the truth.
if grep -qE '^client_ca = ' "$WORK/mqttd.toml"; then
  fail "a MIXED mTLS posture was mapped to client_ca — that silently mandates mTLS on QUIC"
fi
grep -q '^# client_ca = ' "$WORK/mqttd.toml" \
  || fail "the mixed mTLS posture produced neither a mapping nor a commented candidate"
todo 'TLS listeners DISAGREE about client certificates' "$WORK/mqttd.toml"
# The inventory names each TLS listener's MATERIAL as well as its posture. Reporting the
# posture alone left the dropped certfile/keyfile PATHS nowhere in the output, so an operator
# diffing their listener set against the result could not see what was lost — found on
# 2026-08-15 by the property sweep, which asserts every input path survives somewhere.
grep -qE 'my_quic_listener_name \(certfile = .*verify = verify_none' "$WORK/mqttd.toml" \
  || fail "the SECOND TLS listener's material+verify posture was not reported at all"
grep -q '2 TLS listeners were found' "$WORK/mqttd.toml" \
  || fail "the [tls] table's one-per-deployment reach was not stated"
ok "both TLS listeners are reported and the mixed mTLS posture is left to the operator"

# ── 7. secrets are never transformed (ADR 0051 §3 rule 2) ────────────────────────────
# The fixture carries an inline JWT secret and a bridge password. Neither may appear in
# ANY output file — not even copied through.
for secret in emqxsecret not-copied-by-the-converter; do
  if grep -rq "$secret" "$WORK"/*.toml; then
    fail "the secret '$secret' was copied into the converter's output"
  fi
done
grep -q 'secrets are never transformed' "$WORK/mqttd.toml" \
  || fail "the inline JWT secret was dropped without saying so"
ok "no secret material reached the output, and its absence is reported"

# ── 8. the vendor's own default ACL: four rules, four different gaps, zero drops ──────
python3 "$CONV" "$FIX/emqx-adversarial.conf" \
  --acl-file "$FIX/emqx-acl-6.2.2.conf" \
  --out-config "$WORK/adv.toml" --out-acl "$WORK/adv-acl.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the adversarial fixture"
"$MQTTD_BIN" --check-config --config "$WORK/adv.toml" >/dev/null 2>&1 \
  || fail "the adversarial fixture's converted config does not pass --check-config"
todo 'REGULAR EXPRESSION'    "$WORK/adv-acl.toml"   # {re, ...} username
# The instruction handed to the operator must be TRUE: mqtt-auth's glob_match implements
# `*` only (crates/mqtt-auth/src/acl.rs), so telling them `?` works produces a rule that
# matches the literal character `?` and therefore nothing.
if grep -q 'GLOBS (\* and ?)' "$WORK/adv-acl.toml"; then
  fail "the regex TODO still claims mqttd identities support ? — glob_match implements * only"
fi
grep -qE 'GLOBS, and .\*. \(any run of characters' "$WORK/adv-acl.toml" \
  || fail "the regex TODO does not say which glob character is actually special"
todo 'SOURCE IP ADDRESS'     "$WORK/adv-acl.toml"   # {ipaddr, ...}
todo 'EXACT filter-string'   "$WORK/adv-acl.toml"   # {eq, ...}
todo 'EMQX_SECURITY_PROFILE' "$WORK/adv-acl.toml"   # {security_profile, legacy}
# Pin the counts docs/MIGRATION.md quotes, so the document cannot drift away from the
# tool. SIX TODOs, not four: the third vendor rule contributes three (its $SYS topic and
# each of its two {eq, ...} entries). ONE rule is emitted — the $SYS deny, kept for the
# record and reported as inert because mqttd implements no $SYS tree.
acl_todos=$(grep -c 'TODO(migrate)' "$WORK/adv-acl.toml" || true)
acl_rules=$(grep -c '^\[\[rules\]\]' "$WORK/adv-acl.toml" || true)
[[ "$acl_todos" == "6" ]] \
  || fail "the vendor default ACL produced $acl_todos TODO(migrate) lines, not the 6 the docs state"
[[ "$acl_rules" == "1" ]] \
  || fail "the vendor default ACL produced $acl_rules rule(s), not the 1 the docs state"
ok "each of the vendor default ACL's four rules became a TODO, not a drop (6 TODOs, 1 inert rule)"

# ── 8b. the VERBATIM vendor config: stock defaults, and the refusals they must produce ─
# emqx-6.2.2.conf is COMPOSED — nine of its values were deliberately changed from the
# vendor's to switch positive mappings on, and its header lists each one. That makes it
# the wrong fixture to answer "does a real EMQX file convert?", because EMQX's stock
# defaults take the OTHER branch of every one of those mappings. This block runs the
# byte-for-byte vendor article instead (ten example files concatenated, SHA-256s in its
# header) and asserts the branches only a stock config reaches.
python3 "$CONV" "$FIX/emqx-6.2.2-vendor-verbatim.conf" \
  --out-config "$WORK/vendor.toml" --out-acl "$WORK/vendor-acl.toml" \
  --out-bridge "$WORK/vendor-bridge.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the VERBATIM vendor config"
"$MQTTD_BIN" --check-config --config "$WORK/vendor.toml" >/dev/null 2>"$WORK/vendor.err" \
  || { echo "  FAIL — the broker REJECTED the config built from the VERBATIM vendor file:";
       sed 's/^/         /' "$WORK/vendor.err"; exit 1; }
# The stock file sets cacertfile on all three TLS listeners with verify = verify_none and
# fail_if_no_peer_cert = false. Mapping that to client_ca would mandate mTLS on a fleet
# that never presented certificates — the exact fail-open this converter exists to avoid.
grep -qE '^client_ca' "$WORK/vendor.toml" \
  && fail "the VERBATIM vendor config (verify_none everywhere) produced an mTLS MANDATE"
todo 'client certificates were NOT mandatory' "$WORK/vendor.toml"
# mqttd has ONE [tls] table for tls_bind, wss_bind and quic_bind, so all three of the
# vendor's TLS listeners must be named — naming only the first is the defect this fixture
# is shaped to catch, and the vendor ships three.
for lst in my_ssl_listener_name my_wss_listener_name my_quick_listener_name; do
  grep -q "$lst" "$WORK/vendor.toml" \
    || fail "the VERBATIM vendor config's TLS listener $lst is reported NOWHERE"
done
# Stock values that must NOT be read as their opposites: 0 retained messages means
# unlimited in EMQX, and peer_cert_as_username is disabled (no mtls_identity_source).
grep -qE '^max_retained_messages' "$WORK/vendor.toml" \
  && fail "max_retained_messages = 0 (EMQX for UNLIMITED) became a numeric cap"
grep -qE '^mtls_identity_source' "$WORK/vendor.toml" \
  && fail "peer_cert_as_username = disabled produced an mtls_identity_source setting"
ok "the VERBATIM vendor config converts, validates, and refuses every posture it must"

# ── 9. adversarial config: unknown keys, dead subsystems, posture traps ──────────────
todo 'no_such_setting_at_all'    "$WORK/adv.toml"   # unknown key, reported by path
todo 'wildly_invented_section'   "$WORK/adv.toml"   # unknown SECTION, reported by path
todo 'broken_block'              "$WORK/adv.toml"   # malformed input did not crash
todo 'gateway'                   "$WORK/adv.toml"   # no equivalent at all
todo 'plugins'                   "$WORK/adv.toml"
todo 'zones'                     "$WORK/adv.toml"
todo 'has no dtls transport'     "$WORK/adv.toml"
todo 'additional tcp listener'   "$WORK/adv.toml"   # one bind per protocol
# THE posture trap: cacertfile with verify_none must NOT become an mTLS mandate.
grep -q '^client_ca = ' "$WORK/adv.toml" \
  && fail "cacertfile with verify_none was mapped to client_ca — that silently mandates mTLS"
grep -q '^# client_ca = ' "$WORK/adv.toml" \
  || fail "cacertfile with verify_none produced neither a mapping nor a commented candidate"
grep -q 'default = "allow"' "$WORK/adv-acl.toml" \
  || fail "authorization.no_match = allow was not carried into the ACL default"
grep -q 'NOTE:.*no_match was ALLOW' "$WORK/adv.toml" \
  || fail "the open default was carried over without a NOTE saying so"
ok "unknown keys, dead subsystems and both posture traps are reported, not guessed"

# ── 9b. hostile STRINGS: the output must still be loadable TOML ───────────────────────
# The whole-class defect found on 2026-08-14: no value was escaped anywhere. An AD-style
# `CORP\jdoe` and a Windows certificate path each produce output tomllib REJECTS — and a
# TOML parse failure is a WHOLE-DOCUMENT failure, so ONE such user made the broker refuse
# to load the entire migrated policy. This fixture is the default case for a Windows/AD
# estate, not an exotic one.
python3 "$CONV" "$FIX/emqx-hostile-strings.conf" \
  --acl-file "$FIX/emqx-acl-hostile-strings.conf" \
  --out-config "$WORK/hostile.toml" --out-acl "$WORK/hostile-acl.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the hostile-strings fixture"
for f in hostile.toml hostile-acl.toml; do
  python3 - "$WORK/$f" <<'PYEOF' || fail "$f is not valid TOML (an unescaped value)"
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
done
"$MQTTD_BIN" --check-config --config "$WORK/hostile.toml" >/dev/null 2>"$WORK/hostile.err" \
  || { echo "  FAIL — the broker REJECTED the config built from hostile strings:";
       sed 's/^/         /' "$WORK/hostile.err"; exit 1; }
# The escaped forms, exactly: a doubled backslash in a Windows path and in an identity,
# and a backslash-quote inside both an identity and a topic filter.
grep -q 'cert = "C:\\\\emqx\\\\certs\\\\cert.pem"' "$WORK/hostile.toml" \
  || fail "the Windows certfile path was not TOML-escaped"
grep -q 'data_dir = "C:\\\\emqx\\\\data"' "$WORK/hostile.toml" \
  || fail "the Windows data_dir was not TOML-escaped"
grep -q 'identities = \["CORP\\\\jdoe"\]' "$WORK/hostile-acl.toml" \
  || fail "the domain-qualified username was not TOML-escaped in identities"
grep -q 'identities = \["svc\\"quote"\]' "$WORK/hostile-acl.toml" \
  || fail "a double quote in a username was not TOML-escaped"
grep -q 'topics = \["odd\\"topic/#"\]' "$WORK/hostile-acl.toml" \
  || fail "a double quote in a topic filter was not TOML-escaped"
# THE positive half of the mTLS gate, on the one fixture whose TLS listeners AGREE:
# verify_peer + fail_if_no_peer_cert is the only shape where cacertfile may become
# client_ca. Deleting the gate must break this line.
grep -q '^client_ca = "C:\\\\emqx\\\\certs\\\\ca.pem"' "$WORK/hostile.toml" \
  || fail "verify_peer + fail_if_no_peer_cert on the only TLS listener did not become client_ca"
ok "backslash usernames and Windows paths come out escaped, and the config still validates"

# ── 9c. `enable = false` must not switch a decommissioned authenticator back ON ────────
# It is how EMQX's dashboard turns an authenticator off without deleting it, so a
# decommissioned chain entry is a normal thing to find. Translating it as if it were live
# points the migrated broker at a legacy endpoint the operator believes is off.
# The URL must not be a LIVE setting — and it must still be NAMED, because "an http
# authenticator was disabled" without saying which endpoint leaves the operator unable to
# check that the thing they believe is off is the thing that was off (2026-08-15).
if grep -vE '^\s*#' "$WORK/hostile.toml" | grep -q 'legacy-authn'; then
  fail "a DISABLED http authenticator's URL was carried into [security.http_auth]"
fi
grep -q 'legacy-authn' "$WORK/hostile.toml" \
  || fail "the DISABLED authenticator was reported without naming the endpoint it pointed at"
todo "authentication .*has enable = false" "$WORK/hostile.toml"
todo "authorization.sources \[file .*\] has enable = false" "$WORK/hostile.toml"
ok "a disabled authenticator and a disabled authz source are reported by NAME, not activated"

# ── 9d. the silent drops: a second TLS listener, bridge keys, infinity, a missing ACL ──
# Deliberately run with NO --acl-file, so the fixture's own RELATIVE `etc/acl.conf` (which
# is what EMQX ships) cannot be read: the DEFAULT outcome outside the EMQX install root.
python3 "$CONV" "$FIX/emqx-silent-drops.conf" \
  --out-config "$WORK/drops.toml" --out-acl "$WORK/drops-acl.toml" \
  --out-bridge "$WORK/drops-bridge.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the silent-drops fixture"
"$MQTTD_BIN" --check-config --config "$WORK/drops.toml" >/dev/null 2>&1 \
  || fail "the silent-drops fixture's converted config does not pass --check-config"
# (a) the SECOND TLS listener's settings, each named with its listener. Both listeners
#     share identical cert material on purpose, which is what used to suppress every
#     signal that the second one existed.
todo 'listeners.wss.browsers.ssl_options.enable_crl_check' "$WORK/drops.toml"
todo 'listeners.wss.browsers.ssl_options.depth'            "$WORK/drops.toml"
todo 'listeners.wss.browsers accepted TLS 1.2'             "$WORK/drops.toml"
todo 'listeners.ssl.devices.ssl_options.password'          "$WORK/drops.toml"
if grep -q 'not-copied-either' "$WORK"/drops*.toml; then
  fail "the ssl_options.password secret was copied into the output while reporting it"
fi
if grep -qE '^client_ca = ' "$WORK/drops.toml"; then
  fail "a REQUIRED + verify_none pair was mapped to client_ca — mTLS on wss was not required"
fi
grep -q '^# client_ca = ' "$WORK/drops.toml" \
  || fail "the device listener's mTLS mandate produced no commented candidate"
# (b) max_connections = infinity — the vendor's shipped default — leaves a trace.
grep -qE 'NOTE:.*max_connections was .infinity.' "$WORK/drops.toml" \
  || fail "a per-listener max_connections = infinity was skipped with no NOTE"
# (c) every bridge key, including the two that change what the far side receives.
todo 'egress.remote.payload.*PAYLOAD TEMPLATE'  "$WORK/drops-bridge.toml"
todo 'egress.local.retain.*RETAIN override'     "$WORK/drops-bridge.toml"
todo 'bridge_mode'                              "$WORK/drops-bridge.toml"
todo 'clean_start'                              "$WORK/drops-bridge.toml"
todo 'proto_ver'                                "$WORK/drops-bridge.toml"
todo 'resource_opts.batch_size'                 "$WORK/drops-bridge.toml"
# (d) the unreadable ACL source: the gap must be in BOTH deployed files, not on stderr.
todo 'THE AUTHORIZATION POLICY WAS NOT TRANSLATED' "$WORK/drops.toml"
[[ -s "$WORK/drops-acl.toml" ]] \
  || fail "an unreadable ACL source produced NO acl.toml while the config names one"
todo 'NOTHING WAS TRANSLATED INTO THIS FILE'       "$WORK/drops-acl.toml"
grep -q 'default = "deny"' "$WORK/drops-acl.toml" \
  || fail "the placeholder ACL is not deny-by-default"
ok "the second TLS listener, every bridge key, infinity and a missing ACL all leave a trace"

# ── 9e. a DISABLED listener must not become a live bind (2026-08-15) ───────────────────
# `enable` is a real base_listener field in the vendor's own schema (emqx_schema.erl
# base_listener/1 @ 6.2.2, default true, alias `enabled`) and it was UNREAD, so a listener
# the operator had switched off became a live mqttd bind. It is the one flip that opens a
# network port; mqttd's only runtime signal is an INSECURE warning.
if grep -vE '^\s*#' "$WORK/drops.toml" | grep -q '0.0.0.0:1884'; then
  fail "a listener with enable = false was BOUND anyway"
fi
todo 'listeners.tcp.retired_plain has enable = false' "$WORK/drops.toml"
grep -q '0.0.0.0:1884' "$WORK/drops.toml" \
  || fail "the disabled listener was skipped without naming its address"
ok "a listener with enable = false is reported by name and address, and never bound"

# ── 9f. the v2 bridge shape (connectors + actions/sources) translates ──────────────────
# `bridges` is NOT a root in 6.2.2's schema — emqx_conf_schema:roots/0 and
# emqx_bridge_v2_schema:roots/0 give connectors/actions/sources, and `bridges.*` survives
# only through the vendor's v1 upgrade path. A converter that read only the v1 shape wrote
# an upstream with ZERO rules for a current EMQX bridge — forwarding nothing — while the
# section TODO said MQTT-type actions map to mqtt-bridge rules (2026-08-15).
grep -q 'name = "regional"' "$WORK/drops-bridge.toml" \
  || fail "the v2 MQTT connector did not become an upstream"
grep -q 'filter = "telemetry/#"' "$WORK/drops-bridge.toml" \
  || fail "the v2 action's local_topic did not become an out rule's filter"
grep -q 'filter = "commands/#"' "$WORK/drops-bridge.toml" \
  || fail "the v2 source's parameters.topic did not become an in rule's filter"
grep -q 'remap = { prefix = "edge/" }' "$WORK/drops-bridge.toml" \
  || fail "the v2 action's parameters.topic did not become a prefix remap"
todo 'actions.mqtt.push_telemetry.parameters.payload' "$WORK/drops-bridge.toml"
todo 'actions.mqtt.push_telemetry.parameters.retain' "$WORK/drops-bridge.toml"
if grep -vE '^\s*#' "$WORK/drops-bridge.toml" | grep -q 'not-copied-either'; then
  fail "the v2 connector's password reached the bridge config"
fi
ok "the v2 bridge shape translates, and its payload/retain keys are still per-key TODOs"

# ── 10. degenerate inputs ────────────────────────────────────────────────────────────
: > "$WORK/empty.conf"
python3 "$CONV" "$WORK/empty.conf" --out-config "$WORK/empty.toml" >/dev/null 2>&1 \
  || fail "an EMPTY config crashed the converter"
grep -q 'TODO(migrate).*parsed to NOTHING' "$WORK/empty.toml" \
  || fail "an empty config was converted silently instead of reporting it"
"$MQTTD_BIN" --check-config --config "$WORK/empty.toml" >/dev/null 2>&1 \
  || fail "the empty-input config does not pass --check-config"
if python3 "$CONV" "$WORK/does-not-exist.conf" >/dev/null 2>&1; then
  fail "an unreadable input should exit 1, not 0"
fi
ok "an empty config reports itself (exit 0) and a missing one exits 1"

# ── 11. mqtt-bridge's own parser accepts the generated bridge config ─────────────────
#
# TWO DELIBERATE COMPLETIONS FIRST, because the generated file is a DRAFT and the two values
# below are exactly the ones this converter refuses to invent:
#
#   * `[local] url` — the address of YOUR mqttd cluster. Nothing in an EMQX configuration
#     names it, so it is emitted COMMENTED OUT; mqtt-bridge then refuses to start, which is
#     the right way round (the old hard-coded `127.0.0.1:1883` silently pointed the bridge at
#     a loopback broker that is not the one being migrated to). Uncommenting it here is the
#     operator's step, and this assertion is about everything ELSE in the file.
#   * the spool dir, likewise a path.
#
# So: the FIRST assertion is that the draft is INERT (mqtt-bridge rejects it as it stands),
# and the second is that completing those two lines makes the real binary accept it.
if RUST_LOG=info "$BRIDGE_BIN" "$WORK/bridge.toml" > "$WORK/bridge-draft.log" 2>&1; then
  fail "the DRAFT bridge config started mqtt-bridge — [local] url must be inert until set"
fi
grep -qiE 'url|missing field' "$WORK/bridge-draft.log" \
  || fail "mqtt-bridge rejected the draft for some reason other than the unset [local] url"
ok "the draft bridge config is INERT: mqtt-bridge refuses it until [local] url is set"
sed -i.bak "s|/var/lib/mqtt-bridge|$WORK/spool|" "$WORK/bridge.toml"
sed -i.bak2 's|^# url = "127.0.0.1:1883".*|url = "127.0.0.1:1883"|' "$WORK/bridge.toml"
grep -qE '^url = "127.0.0.1:1883"' "$WORK/bridge.toml" \
  || fail "the commented [local] url candidate is not there to uncomment"
mkdir -p "$WORK/spool"
RUST_LOG=info "$BRIDGE_BIN" "$WORK/bridge.toml" > "$WORK/bridge.log" 2>&1 &
BRIDGE=$!
trap 'kill $BRIDGE 2>/dev/null || true; rm -rf "$WORK"' EXIT
for _ in $(seq 1 50); do
  grep -q 'starting mqtt-bridge' "$WORK/bridge.log" && break
  sleep 0.1
done
# "starting mqtt-bridge" is logged only AFTER BridgeConfig::parse_toml + validate() pass,
# so its presence is the real binary's verdict on the generated file.
if ! grep -q 'starting mqtt-bridge' "$WORK/bridge.log"; then
  echo "  FAIL — mqtt-bridge REJECTED the converted bridge config:"
  tail -5 "$WORK/bridge.log" | sed 's/^/         /'
  exit 1
fi
kill $BRIDGE 2>/dev/null || true
wait $BRIDGE 2>/dev/null || true   # reap it quietly; an unreaped job prints "Terminated"
ok "mqtt-bridge accepts the converted bridge config"

# ── 11b. a bridge that used TLS: the UPSTREAM URL is the posture, so it cannot be live ─
#
# Round 3 commented `[upstreams.tls]` out (its paths are EMQX's, on the EMQX host) and left the
# upstream `url` LIVE. mqtt-bridge's tls block is OPTIONAL and ABSENT MEANS PLAINTEXT, so
# completing the draft exactly as the file instructs — `[local] url` and a spool dir, the two
# values it names — produced a bridge that connected to a TLS peer IN THE CLEAR, carrying its
# CONNECT and username. Commenting the tls block IS the posture change, so the line whose
# liveness decides the posture must be inert too. Measured against the real binary, 2026-08-15.
cat > "$WORK/tlsbridge.conf" <<'CONF'
node.data_dir = "/var/lib/emqx"
connectors.mqtt.up { server = "10.9.9.9:8883", username = "u",
  ssl { enable = true, cacertfile = "/e/ca.pem", verify = verify_peer } }
actions.mqtt.a1 { connector = up, parameters { topic = "up/t" }, local_topic = "loc/#" }
CONF
python3 scripts/migrate/from-emqx.py "$WORK/tlsbridge.conf" \
  --out-config "$WORK/tlsbridge.toml" --out-bridge "$WORK/tlsbridge-bridge.toml" >/dev/null
if grep -qE '^url = "10\.9\.9\.9:8883"' "$WORK/tlsbridge-bridge.toml"; then
  fail "a TLS upstream got a LIVE plaintext url — the posture downgrade is live"
fi
grep -qF '# url = "10.9.9.9:8883"' "$WORK/tlsbridge-bridge.toml" \
  || fail "the TLS upstream's url is not emitted as an inert candidate"
grep -qF 'in the CLEAR to a peer that expected TLS' "$WORK/tlsbridge-bridge.toml" \
  || fail "the reason the upstream url is inert is not stated"
grep -qF '/e/ca.pem' "$WORK/tlsbridge-bridge.toml" \
  || fail "the EMQX side's CA path was dropped instead of named"
# ...and completing ONLY the two paths the file tells the operator to complete must NOT yield a
# bridge that starts and connects in cleartext.
sed -i.bak "s|/var/lib/mqtt-bridge|$WORK/spool2|" "$WORK/tlsbridge-bridge.toml"
sed -i.bak2 's|^# url = "127.0.0.1:1883".*|url = "127.0.0.1:1883"|' "$WORK/tlsbridge-bridge.toml"
mkdir -p "$WORK/spool2"
if RUST_LOG=info "$BRIDGE_BIN" "$WORK/tlsbridge-bridge.toml" > "$WORK/tlsbridge.log" 2>&1; then
  fail "the completed-as-instructed draft STARTED with a plaintext upstream to a TLS peer"
fi
grep -qiE 'url|missing field' "$WORK/tlsbridge.log" \
  || fail "mqtt-bridge refused the TLS-bridge draft for some other reason than the inert url"
ok "a TLS upstream stays inert: completing the named paths cannot produce a cleartext upstream"

# ── 11c. a LIVE authenticator on a non-http/jwt backend names its credential store ─────
#
# `report_unread_authn_keys()` was wired on the http and jwt branches only, so every other key
# of a live authenticator vanished under a reassuring per-mechanism TODO — including on THIS
# REPOSITORY'S OWN pinned fixture, whose `backend = mysql` entry names the server, database and
# query that authenticated every client. Asserted on the fixture, so the gap cannot come back
# unnoticed the way it survived "every expected TODO(migrate) marker is present".
for needle in 'mysql:3306' 'SELECT password_hash' 'NAMES THE CREDENTIAL STORE'; do
  grep -qF "$needle" "$WORK/mqttd.toml" \
    || fail "the mysql authenticator's '$needle' is nowhere in the output"
done
ok "a non-http/jwt authenticator's credential store is named, not summarised away"

# ── 11d. one claim, one statement: verify_claims is mapped OR reported, never both ──────
#
# Round 3's remediation reintroduced its own contradiction class in the opposite direction:
# `verify_claims` was absent from AUTHN_JWT_READ, so the leaf reporter enumerated its claims and
# the same file emitted `issuer = "…"  # from: … verify_claims.iss` AND a TODO saying that exact
# claim has no mqttd equivalent. Both readings are actionable; one of them is wrong.
cat > "$WORK/claims.conf" <<'CONF'
node.data_dir = "/var/lib/emqx"
authentication = [ { mechanism = jwt,
    verify_claims = { iss = "ISS-SENTINEL", aud = "AUD-SENTINEL", tenant = "TENANT-SENTINEL" } } ]
CONF
python3 scripts/migrate/from-emqx.py "$WORK/claims.conf" --out-config "$WORK/claims.toml" >/dev/null
grep -qF 'issuer = "ISS-SENTINEL"' "$WORK/claims.toml" \
  || fail "verify_claims.iss did not map onto [security.jwt] issuer"
if grep -qF "verify_claims.iss = 'ISS-SENTINEL': no mqttd equivalent" "$WORK/claims.toml"; then
  fail "the same file maps verify_claims.iss AND reports it as having no equivalent"
fi
grep -qF 'TENANT-SENTINEL' "$WORK/claims.toml" \
  || fail "a claim mqttd cannot check was dropped instead of reported"
ok "a mapped claim is not also reported as unmappable, and an unmapped one is still named"

# ── 11e. a bind mqttd cannot bind is not a bind ─────────────────────────────────────────
#
# `--check-config` accepts ANY string in a bind, so the verification the docs point at verified
# nothing here: `:8085` (host omitted, which EMQX's own ip_port accepts) failed at STARTUP, and
# a non-scalar `bind` was `str()`-ed into the Python repr `"['0.0.0.0:1883']"` — a live value
# that appears NOWHERE in the input, which the provenance invariant missed because for a
# `*_bind` it compares only the port.
cat > "$WORK/binds.conf" <<'CONF'
node.data_dir = "/var/lib/emqx"
listeners.tcp.a { bind = ["0.0.0.0:1883"] }
listeners.ws.w { bind = ":8085" }
CONF
python3 scripts/migrate/from-emqx.py "$WORK/binds.conf" --out-config "$WORK/binds.toml" >/dev/null
if grep -qE "^(plaintext|ws)_bind = " "$WORK/binds.toml"; then
  fail "an address mqttd cannot bind was emitted LIVE"
fi
grep -qF 'not a single address but a list' "$WORK/binds.toml" \
  || fail "a non-scalar bind was reshaped instead of refused"
grep -qF 'names NO host' "$WORK/binds.toml" \
  || fail "a host-less bind was not reported as unbindable"
"$MQTTD_BIN" --check-config --config "$WORK/binds.toml" >/dev/null 2>&1 \
  || fail "the broker rejected the config built from unbindable listener addresses"
ok "an unbindable address comes out inert, and the reason names the input value"

# ── 11b. a LITERAL %c/%i in an acl.conf topic: refused, not turned into a grant ───────
# EMQX 5/6 substitutes only ${...} placeholders (the pinned acl.conf schema @ 6.2.2 lists
# them; %c/%i are not among them), so a topic carrying %c matched those bytes LITERALLY.
# mqttd substitutes %c/%i in EVERY rule's topics with no escape, so carrying the filter
# across turns a rule on one literal topic into a live per-client grant the source never
# gave. The Mosquitto converter refuses this on a plain `topic` line; until issue #297 this
# converter emitted it with only a fail-closed caveat beside it.
cat > "$WORK/literal.conf" <<'CONF'
node.data_dir = "/var/lib/emqx"
authorization { no_match = deny, sources = [ { type = file, path = "acl.conf" } ] }
CONF
cat > "$WORK/literal-acl.conf" <<'CONF'
{allow, {username, "alice"}, publish, ["c/%c/x"]}.
{allow, {username, "alice"}, subscribe, ["devices/${clientid}/cmd"]}.
CONF
python3 "$CONV" "$WORK/literal.conf" --acl-file "$WORK/literal-acl.conf" \
  --out-config "$WORK/literal.toml" --out-acl "$WORK/literal-out.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the literal-%c fixture"
# comment lines stripped: the TODO's verbatim quote of the refused source rule is the
# PRESCRIBED handling, only a LIVE topics = [...] line is the defect.
if grep -vE '^\s*#' "$WORK/literal-out.toml" | grep -q '"c/%c/x"'; then
  fail "a topic EMQX matched LITERALLY (c/%c/x) was emitted as a SUBSTITUTING mqttd rule"
fi
grep -qF "c/%c/x" "$WORK/literal-out.toml" \
  || fail "the refused literal topic is not named anywhere in the output"
grep -qF "LITERALLY" "$WORK/literal-out.toml" \
  || fail "the refusal does not state that EMQX matched those bytes literally"
# ...and a real ${clientid} placeholder must still translate — the refusal must not
# swallow the construct it exists to protect.
grep -q '"devices/%c/cmd"' "$WORK/literal-out.toml" \
  || fail "a genuine \${clientid} placeholder stopped translating to %c"
ok "a literal %c topic is refused and named; \${clientid} still translates"

# ── 11c. a JWKS authenticator names its endpoint ──────────────────────────────────────
# "with JWKS" without the URL left the operator unable to check that the provider the
# advice reconfigures around is the provider they believe was in force — the same rule
# report_unread_authn_keys already applies to every other authenticator's server/url.
cat > "$WORK/jwks.conf" <<'CONF'
node.data_dir = "/var/lib/emqx"
authentication = [
  { mechanism = jwt, use_jwks = true, endpoint = "https://idp.example.com/keys.json" }
]
CONF
python3 "$CONV" "$WORK/jwks.conf" --out-config "$WORK/jwks.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the JWKS fixture"
grep -qF 'https://idp.example.com/keys.json' "$WORK/jwks.toml" \
  || fail "the JWKS endpoint URL is not named anywhere in the output"
grep -qF '[security.oidc]' "$WORK/jwks.toml" \
  || fail "the JWKS TODO no longer points at the [security.oidc] path"
ok "a JWKS authenticator's endpoint is named, beside the [security.oidc] remediation"

# ── 12. THE assertion: the real broker boots on the converted ACL ────────────────────
# Run twice: once on the vendor fixture's policy, and once on the hostile-strings policy —
# a backslash identity and a double-quoted topic filter have to survive the broker's own
# ACL loader, not only tomllib.
boot_on_acl() {  # $1 = acl file, $2 = what it proves
  local acl="$1" what="$2" port hport attempt
  # Issue #360: the bind(0)/close/reuse port mint is TOCTOU — any process on the
  # runner can take the port between the close and the broker's own bind. Bounded
  # retry with FRESH ports on AddrInUse, and the failure report distinguishes
  # "could not bind" from "rejected the config" (the incident misattributed a
  # bind race as an ACL rejection).
  for attempt in 1 2 3; do
    port=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
    hport=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
    MQTTD_ACL_FILE="$acl" MQTTD_PLAINTEXT_BIND="127.0.0.1:$port" \
      MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
      MQTTD_ALLOW_ANONYMOUS=1 MQTTD_HEALTH_BIND="127.0.0.1:$hport" RUST_LOG=warn \
      "$MQTTD_BIN" > "$WORK/boot.log" 2>&1 &
    BROKER=$!
    trap 'kill $BROKER 2>/dev/null || true; rm -rf "$WORK"' EXIT
    for _ in $(seq 1 100); do
      curl -fsS "http://127.0.0.1:$hport/readyz" >/dev/null 2>&1 && break
      sleep 0.1
    done
    if curl -fsS "http://127.0.0.1:$hport/readyz" >/dev/null 2>&1; then
      kill "$BROKER" 2>/dev/null || true
      wait "$BROKER" 2>/dev/null || true   # reap it quietly; an unreaped job prints "Terminated"
      echo "  ok   — the broker booted on $what"
      return 0
    fi
    kill "$BROKER" 2>/dev/null || true
    wait "$BROKER" 2>/dev/null || true
    if grep -qE "AddrInUse|Address already in use" "$WORK/boot.log"; then
      echo "  note — bind race (AddrInUse) on attempt $attempt; retrying with fresh ports"
      continue
    fi
    echo "  FAIL — the broker REJECTED $what:"
    tail -5 "$WORK/boot.log" | sed 's/^/         /'
    exit 1
  done
  echo "  FAIL — the broker could NOT BIND after 3 attempts (AddrInUse each time — runner port pressure, not a config rejection):"
  tail -5 "$WORK/boot.log" | sed 's/^/         /'
  exit 1
}
boot_on_acl "$WORK/acl.toml" "the converted ACL"
boot_on_acl "$WORK/hostile-acl.toml" \
  "the ACL holding a backslash identity and a quoted topic filter"
# ── the PROPERTY SWEEP ──────────────────────────────────────────────────────────────────
# Everything above is example-based: one input, a list of greps. That shape catches a
# regression exactly where a reviewer already looked and is blind everywhere else — which is
# how the same first-listener-only TLS defect was found three times in three converters, each
# harness having only ever fed its converter ONE ordering of ONE listener set. The sweep
# generates many inputs (listener ORDER, enable flags, mTLS postures, no_match postures,
# truststore presence) and asserts one invariant per defect CLASS on every one of them:
# nothing silently dropped, nothing disabled activated, no claim contradicting the value it
# describes, no dangling `step N`, and `--check-config` on every generated config.
python3 scripts/migrate/property_sweep.py emqx --mqttd "$MQTTD_BIN" \
  || fail "the emqx property sweep found a case the fixture tests cannot see"

# ── the FUZZ pass ───────────────────────────────────────────────────────────────────────
# The generators above enumerate axes their author thought of, which is how round 2's
# blocking defect survived round 1. This pass does not think: it mutates each fixture
# mechanically (delete lines, truncate mid-structure, permute blocks, flip enable flags, swap
# transports) and asserts only what must hold for ANY byte sequence — the converter EXITS,
# 0 or 1, with a message; whatever it writes is valid TOML; and no live security-relevant
# line lacks provenance. It is how the HOCON reader's infinite loop was found (a two-line
# input: `authentication = [` then `}`), which no example-based test could have produced.
python3 scripts/migrate/property_sweep.py emqx --fuzz 40 \
  || { echo "  FAIL — the emqx fuzz pass found an input this converter wedges, crashes or invents on";
       exit 1; }

echo "EMQX MIGRATE OK"
