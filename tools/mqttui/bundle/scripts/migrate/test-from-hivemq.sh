#!/usr/bin/env bash
# The HiveMQ converter's output must be accepted by the broker, not merely well-formed.
#
# A migration tool that emits plausible-looking TOML the broker then rejects is worse than
# none: it burns the evaluation it was meant to enable. So this feeds the vendor-derived
# fixtures through the converter and then asks the REAL binary to accept the results —
# `mqttd --check-config` on the config (ADR 0051 §3's third rule, which the Mosquitto
# harness never actually enforced) and a real broker BOOT on the translated ACL.
#
# It also asserts the contract's other half, which is the whole reason the tool is trusted:
# untranslatable input comes out as a TODO(migrate) line, never a silent drop and never a
# crash — proven on an adversarial fixture (unknown elements, HiveMQ Enterprise constructs
# whose schema is not open source, posture traps), an empty document and malformed XML.
set -euo pipefail
cd "$(dirname "$0")/../.."

MQTTD_BIN="${MQTTD_BIN:-target/debug/mqttd}"
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: $MQTTD_BIN not built"; exit 2; }

FIX=scripts/migrate/fixtures
CONV=scripts/migrate/from-hivemq.py

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

ok()   { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }
todo() { grep -q "TODO(migrate).*$1" "$2" || fail "no TODO(migrate) for: $1"; }

# ── 1. the vendor fixture converts ───────────────────────────────────────────────────
python3 "$CONV" "$FIX/hivemq-2026.5-config.xml" \
  --credentials "$FIX/hivemq-credentials-4.6.16.xml" \
  --out-config "$WORK/mqttd.toml" --out-acl "$WORK/acl.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the vendor fixture"
[[ -s "$WORK/mqttd.toml" && -s "$WORK/acl.toml" ]] \
  || fail "the converter produced an empty output file"
ok "the vendor fixture (HiveMQ CE @ 2026.5 samples + file-RBAC @ 4.6.16) converts"

# ── 2. both converted documents are valid TOML ───────────────────────────────────────
for f in mqttd.toml acl.toml; do
  python3 - "$WORK/$f" <<'PYEOF' || fail "$f is not valid TOML"
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
done
ok "the converted config and ACL both parse as TOML"

# ── 3. THE assertion ADR 0051 §3 demands: the config passes --check-config ────────────
"$MQTTD_BIN" --check-config --config "$WORK/mqttd.toml" >/dev/null 2>"$WORK/check.err" \
  || { echo "  FAIL — the broker REJECTED the converted config:";
       sed 's/^/         /' "$WORK/check.err"; exit 1; }
ok "the converted config passes 'mqttd --check-config'"

# ── 4. the mappings that must be present ─────────────────────────────────────────────
# Each is a rule that translates; deleting the rule must break this test (the mutation
# proof recorded in docs/delivery/0051-evaluation-readiness.md).
grep -q 'plaintext_bind = "0.0.0.0:1883"' "$WORK/mqttd.toml" || fail "tcp-listener did not become plaintext_bind"
grep -q 'ws_bind = "0.0.0.0:8000"' "$WORK/mqttd.toml"        || fail "websocket-listener did not become ws_bind"
grep -q 'tls_bind = "0.0.0.0:8883"' "$WORK/mqttd.toml"       || fail "tls-tcp-listener did not become tls_bind"
grep -q 'max_packet_size = 268435460' "$WORK/mqttd.toml"     || fail "mqtt/packets/max-packet-size did not map"
grep -q 'receive_maximum = 10' "$WORK/mqttd.toml"            || fail "server-receive-maximum did not become receive_maximum"
grep -q 'topic_alias_max = 5' "$WORK/mqttd.toml"             || fail "topic-alias/max-per-client did not map"
grep -q 'max_queued_messages = 1000' "$WORK/mqttd.toml"      || fail "queued-messages/max-queue-size did not map"
# HiveMQ `discard` drops the INCOMING message, which mqttd spells reject-newest. Getting
# this backwards silently changes which message is lost at the cap.
grep -q 'queue_overflow = "reject-newest"' "$WORK/mqttd.toml" \
  || fail "queued-messages strategy 'discard' did not become queue_overflow=reject-newest"
# client-authentication-mode REQUIRED is the ONLY case where client_ca may be set.
grep -q '^client_ca = ' "$WORK/mqttd.toml" \
  || fail "client-authentication-mode REQUIRED did not become a client_ca mandate"
ok "listeners, limits, the queue-overflow arm and the mTLS mandate all mapped"

# ── 5. the ACL: deny-by-default, roles flattened, substitutions translated ───────────
grep -q 'default = "deny"' "$WORK/acl.toml" || fail "the translated ACL is not deny-by-default"
# ${{clientid}} -> %c and ${{username}} -> %i, or every templated rule matches nothing.
grep -q '"data/%c/#"' "$WORK/acl.toml" || fail "file-RBAC \${{clientid}} was not translated to %c"
grep -q '"incoming/%i/actions"' "$WORK/acl.toml" || fail "file-RBAC \${{username}} was not translated to %i"
# Roles are flattened onto members: role1 has 5 permissions and 2 members, superuser 1 and 1.
[[ "$(grep -c '^\[\[rules\]\]' "$WORK/acl.toml")" == "11" ]] \
  || fail "expected 11 flattened rules (5x2 + 1x1), got $(grep -c '^\[\[rules\]\]' "$WORK/acl.toml")"
grep -q 'identities = \["user1"\]' "$WORK/acl.toml"      || fail "user1 got no rules"
grep -q 'identities = \["admin-user"\]' "$WORK/acl.toml" || fail "admin-user got no rules"
ok "the ACL is deny-by-default, roles are flattened onto both users, and \${{..}} became %c/%i"

# ── 6. the TODO(migrate) markers that must be present ────────────────────────────────
# The issue's acceptance criterion: "expected output + TODO markers out".
todo 'JAVA KEYSTORE'                "$WORK/mqttd.toml"  # JKS cannot become PEM
todo 'subscription identifiers'     "$WORK/mqttd.toml"  # a stated mqttd gap
todo 'no byte-rate limiter'         "$WORK/mqttd.toml"  # incoming-bandwidth-throttling
todo 'cannot cap the maximum'       "$WORK/mqttd.toml"  # max-qos
todo 'no telemetry'                 "$WORK/mqttd.toml"  # anonymous-usage-statistics
todo 'subprotocol'                  "$WORK/mqttd.toml"  # mqttv3.1 would fail the handshake
todo 'ROLES WERE FLATTENED'         "$WORK/acl.toml"    # groups need OIDC/HTTP auth
todo 'NO QoS qualifier'             "$WORK/acl.toml"    # the <qos> constraint is lost
todo 'NO retain qualifier'          "$WORK/acl.toml"    # the <retain> constraint is lost
todo 'shared subscriptions'         "$WORK/acl.toml"    # <shared-subscription> is lost
todo 'PASSWORDS WERE NOT READ'      "$WORK/acl.toml"
grep -q 'keytool -importkeystore' "$WORK/mqttd.toml" \
  || fail "the JKS TODO carries no extraction recipe"
ok "every expected TODO(migrate) marker is present, JKS recipe included"

# ── 7. secrets are never transformed (ADR 0051 §3 rule 2) ────────────────────────────
# The fixtures carry keystore passwords and two PBKDF2 password hashes. None may appear
# in ANY output file — not even copied through.
for secret in password-keystore password-key password-truststore \
              'FY12nwpUEbBK9EKQ' 'PL2FLqfpdhONG7qXjAMmdVn4wlMiXnypdXiFW09zqorFhKgoiixFQw2EVJJfE9Zn79q45V7Xpc6JeKLp0ntmYA'; do
  if grep -rq "$secret" "$WORK"/*.toml; then
    fail "the secret '$secret' was copied into the converter's output"
  fi
done
# ...and every username gets a re-enrolment command instead.
grep -q 'mqttd --hash-password user1' "$WORK/acl.toml"      || fail "no re-hash command for user1"
grep -q 'mqttd --hash-password admin-user' "$WORK/acl.toml" || fail "no re-hash command for admin-user"
ok "no keystore password or password hash reached the output; both users get a re-enrol line"

# ── 8. adversarial config: unknown elements, Enterprise constructs, posture traps ─────
python3 "$CONV" "$FIX/hivemq-adversarial.xml" --out-config "$WORK/adv.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the adversarial fixture"
"$MQTTD_BIN" --check-config --config "$WORK/adv.toml" >/dev/null 2>&1 \
  || fail "the adversarial fixture's converted config does not pass --check-config"
todo 'invented-element/some-key'    "$WORK/adv.toml"  # unknown element, reported by PATH
todo 'mqtt/no-such-thing/deeper'    "$WORK/adv.toml"  # nested unknown element, by PATH
todo '<cluster>'                    "$WORK/adv.toml"  # Enterprise: schema not open source
todo '<control-center>'             "$WORK/adv.toml"
todo '<license>'                    "$WORK/adv.toml"
todo '<extensions>'                 "$WORK/adv.toml"  # no extension SDK
todo '<ese>'                        "$WORK/adv.toml"  # Enterprise Security Extension
todo '<bridge>'                     "$WORK/adv.toml"  # -> mqtt-bridge, see docs/MIGRATION.md
todo '<overload-protection>'        "$WORK/adv.toml"
todo 'has no such transport'        "$WORK/adv.toml"  # mqtt-sn-listener
todo 'additional tcp-listener'      "$WORK/adv.toml"  # one bind per protocol
todo 'IN-MEMORY'                    "$WORK/adv.toml"  # persistence mode
todo 'TLS 1.3 ONLY'                 "$WORK/adv.toml"  # a TLSv1.2 protocol list
# The OTHER arm of the queue-overflow pair, which the vendor-derived fixture cannot reach
# (it ships `discard`). Getting the pair backwards would silently change WHICH message is
# lost, so both arms need an assertion, not just the one.
grep -q 'queue_overflow = "drop-oldest"' "$WORK/adv.toml" \
  || fail "queued-messages strategy 'discard-oldest' did not become queue_overflow=drop-oldest"
# And the series it points the operator at must be the one the broker actually exports:
# every broker metric carries the `mqttd_` registry prefix and a counter carries `_total`.
grep -q 'mqttd_publish_dropped_total{reason="queue-overflow"}' "$WORK/adv.toml" \
  || fail "the drop-oldest NOTE cites a metric name the broker does not export"
# A per-listener TLS setting must name the listener it came from — HiveMQ listener elements
# have no name attribute, so without the port two <tls-tcp-listener>s would collapse into
# one deduplicated message and the operator could not tell which listener lost the setting.
todo 'listeners/tls-tcp-listener (port 8883)/tls/handshake-timeout' "$WORK/adv.toml"
# THE posture trap: client-authentication-mode OPTIONAL must NOT become an mTLS mandate.
grep -q '^client_ca = ' "$WORK/adv.toml" \
  && fail "client-authentication-mode OPTIONAL was mapped to client_ca — that silently mandates mTLS"
grep -q '^# client_ca = ' "$WORK/adv.toml" \
  || fail "OPTIONAL client auth produced neither a mapping nor a commented candidate"
grep -q 'no --credentials was given' "$WORK/adv.toml" \
  || fail "a CE config with no auth extension was converted without saying so"
ok "unknown elements, Enterprise constructs and the cert-optional trap are all reported"

# ── 8b. EVERY TLS listener is read, not just the first in document order ───────────────
# THE blocking defect of 2026-08-14, in the fail-OPEN direction and on the most
# security-relevant construct there is. hivemq-multi-tls.xml is the ordinary HiveMQ shape:
# a <tls-websocket-listener> first with client-authentication-mode NONE, then a
# <tls-tcp-listener> that REQUIRES a client certificate, has a <truststore> and admits
# TLSv1.2. Reading listener[0] only made all three vanish with no TODO — and because both
# listeners share ONE keystore path, the "different keystores" TODO stayed silent too, so
# the second listener was mentioned nowhere at all. `--check-config` passes on such a file:
# the operator migrates a broker that required client certificates and deploys one that
# accepts those clients with none.
python3 "$CONV" "$FIX/hivemq-multi-tls.xml" --out-config "$WORK/multi.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the multi-TLS fixture"
"$MQTTD_BIN" --check-config --config "$WORK/multi.toml" >/dev/null 2>&1 \
  || fail "the multi-TLS fixture's converted config does not pass --check-config"
todo 'TLS listeners DISAGREE about client certificates' "$WORK/multi.toml"
grep -q 'REQUIRED on tls-tcp-listener on 0.0.0.0:8883' "$WORK/multi.toml" \
  || fail "the mTLS MANDATE on the second TLS listener was not reported at all"
grep -q '2 TLS listeners were found' "$WORK/multi.toml" \
  || fail "the second TLS listener was not reported as sharing the one [tls] table"
# mqttd has no cert-optional mode and one posture for every TLS transport, so neither arm
# is a mapping: the candidate must be COMMENTED OUT (the #162 precedent).
if grep -qE '^client_ca = ' "$WORK/multi.toml"; then
  fail "a mixed REQUIRED/NONE posture was mapped to client_ca — that mandates mTLS on wss"
fi
grep -q '^# client_ca = ' "$WORK/multi.toml" \
  || fail "the REQUIRED listener's mandate produced no commented candidate either"
# The second listener's truststore is the trust anchor BEHIND that mandate: it has to be in
# the extraction recipe, or the operator cannot produce the file client_ca would name.
grep -q 'keytool -list -rfc -keystore /opt/hivemq/conf/device-truststore.jks' "$WORK/multi.toml" \
  || fail "the second listener's truststore is missing from the extraction recipe"
grep -q "tls-tcp-listener on 0.0.0.0:8883 accepted \['TLSv1.3', 'TLSv1.2'\]" "$WORK/multi.toml" \
  || fail "the second listener's TLSv1.2 acceptance was not reported"
ok "the second TLS listener's mandate, truststore and TLS 1.2 are all reported"

# ── 8c. hostile STRINGS in the credentials file: the ACL must still load ───────────────
# The whole-class defect: no value was escaped anywhere, so a file-RBAC `CORP\jdoe` gave
# `identities = ["CORP\jdoe"]` — which tomllib REJECTS. A TOML parse failure is a
# WHOLE-DOCUMENT failure, so one AD-style user made the broker refuse the entire policy.
python3 "$CONV" "$FIX/hivemq-2026.5-config.xml" \
  --credentials "$FIX/hivemq-hostile-credentials.xml" \
  --out-config "$WORK/hostile.toml" --out-acl "$WORK/hostile-acl.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the hostile-credentials fixture"
for f in hostile.toml hostile-acl.toml; do
  python3 - "$WORK/$f" <<'PYEOF' || fail "$f is not valid TOML (an unescaped value)"
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
done
"$MQTTD_BIN" --check-config --config "$WORK/hostile.toml" >/dev/null 2>&1 \
  || fail "the config built beside the hostile credentials does not pass --check-config"
grep -q 'identities = \["CORP\\\\jdoe"\]' "$WORK/hostile-acl.toml" \
  || fail "the domain-qualified username was not TOML-escaped in identities"
grep -q 'identities = \["svc\\"quote"\]' "$WORK/hostile-acl.toml" \
  || fail "a double quote in a username was not TOML-escaped"
grep -q 'topics = \["odd\\"topic/#"\]' "$WORK/hostile-acl.toml" \
  || fail "a double quote in a topic filter was not TOML-escaped"
# The re-enrolment line is meant to be COPIED AND RUN: unquoted, the backslash escapes the
# j and enrols a different username.
grep -q "mqttd --hash-password 'CORP\\\\jdoe'" "$WORK/hostile-acl.toml" \
  || fail "the re-enrolment command does not shell-quote a backslash username"
# ...and the unknown elements on the file that IS the authorization policy.
todo 'UNKNOWN top-level element <extra-policy-section>' "$WORK/hostile-acl.toml"
todo 'carries <enabled>false</enabled>'                 "$WORK/hostile-acl.toml"
ok "backslash and quoted values come out escaped, and unknown policy elements are reported"

# ── 8c-bis. a user file-RBAC had SWITCHED OFF must not get live allow rules ─────────────
# Round 1 fixed this class on EMQX's authenticators, authz sources and bridges; round 2
# found it on EMQX's listeners; the property sweep found it here, where the previous fix
# reported <enabled>false</enabled> and then said in the same breath that "their rules WERE
# still emitted below". Under the contract a mapping that changes SECURITY POSTURE is not a
# mapping: the grant is emitted COMMENTED OUT with a TODO (2026-08-15).
if grep -vE '^\s*#' "$WORK/hostile-acl.toml" | grep -q 'CORP\\\\retired'; then
  fail "a user with <enabled>false</enabled> got a LIVE allow rule"
fi
grep -q '# identities = \["CORP\\\\retired"\]' "$WORK/hostile-acl.toml" \
  || fail "the disabled user's rule was dropped instead of emitted as a commented candidate"
grep -q '# TODO(migrate): this user was <enabled>false</enabled>' "$WORK/hostile-acl.toml" \
  || fail "the commented-out rule carries no TODO saying why"
# ...and their re-enrolment command must be commented too, or the account is re-created.
if grep -E "^#   printf" "$WORK/hostile-acl.toml" | grep -q 'CORP\\\\retired'; then
  fail "a switched-off user's re-enrolment command is offered as a runnable line"
fi
# The ENABLED users' rules and commands are still live.
grep -q '^identities = \["CORP\\\\jdoe"\]' "$WORK/hostile-acl.toml" \
  || fail "an enabled user lost their rule"
ok "a switched-off user's grant and re-enrolment line are commented out, not activated"

# ── 8c-ter. REQUIRED mTLS with NO truststore: every numbered step must exist ────────────
# The output referenced `step 2` twice — in `client_ca = … # TODO(migrate): step 2` and in a
# NOTE calling the path "a placeholder until you run step 2" — while the step-2 block was
# emitted only inside the truststore loop. With REQUIRED client auth and no <truststore>
# (which the vendor XSD permits: minOccurs="0") the recipe ran 1 -> 3 and step 2 did not
# exist. The broker then refuses to start on the unreadable placeholder, and the cheapest way
# out of a start failure is to comment out the line you have no recipe for — silently
# dropping a live mTLS mandate (2026-08-15).
cat > "$WORK/req-no-ts.xml" <<'XML'
<?xml version="1.0"?>
<hivemq>
  <listeners>
    <tls-tcp-listener>
      <port>8883</port>
      <bind-address>0.0.0.0</bind-address>
      <tls>
        <keystore>
          <path>/opt/hivemq/conf/only.jks</path>
          <password>x</password>
        </keystore>
        <client-authentication-mode>REQUIRED</client-authentication-mode>
      </tls>
    </tls-tcp-listener>
  </listeners>
</hivemq>
XML
python3 "$CONV" "$WORK/req-no-ts.xml" --out-config "$WORK/reqnots.toml" >/dev/null 2>&1 \
  || fail "the converter failed on REQUIRED mTLS with no truststore"
"$MQTTD_BIN" --check-config --config "$WORK/reqnots.toml" >/dev/null 2>&1 \
  || fail "the REQUIRED-without-truststore config does not pass --check-config"
grep -q 'client_ca = "/etc/mqttd/tls/client-ca.crt"' "$WORK/reqnots.toml" \
  || fail "a unanimous REQUIRED posture did not map to client_ca"
python3 - "$WORK/reqnots.toml" <<'PYEOF' || fail "the TLS recipe references a step it never printed"
import re, sys
text = open(sys.argv[1], encoding="utf-8").read()
defined = {int(m.group(1)) for m in
           (re.match(r"#\s*#?\s*(\d+)\.\s", l.strip()) for l in text.splitlines()) if m}
referenced = set()
for m in re.finditer(r"\bsteps?\s+(\d+(?:\s*\+\s*\d+)*)", text, re.I):
    referenced.update(int(n) for n in re.findall(r"\d+", m.group(1)))
missing = sorted(referenced - defined)
if missing:
    print(f"  dangling step reference(s) {missing}; printed {sorted(defined)}")
    raise SystemExit(1)
PYEOF
todo 'NO <truststore> was configured on any TLS listener' "$WORK/reqnots.toml"
grep -q "JVM's DEFAULT trust store" "$WORK/reqnots.toml" \
  || fail "the missing truststore's real fallback (the JVM default store) is not named"
ok "REQUIRED mTLS with no truststore maps, names its anchor gap, and every step it cites exists"

# ── 8d. a credentials file with no policy in it says so ────────────────────────────────
# The mirror-image gap: valid XML with no <users>/<roles> (the extension's OTHER file, for
# instance) used to produce an ACL with `default = "deny"`, zero rules and NO TODO — an
# entire authorization policy missing, in silence.
printf '<file-rbac></file-rbac>\n' > "$WORK/nopolicy.xml"
python3 "$CONV" "$FIX/hivemq-2026.5-config.xml" --credentials "$WORK/nopolicy.xml" \
  --out-config "$WORK/np.toml" --out-acl "$WORK/np-acl.toml" >/dev/null 2>&1 \
  || fail "an empty credentials document crashed the converter"
todo 'no <users> section was found'  "$WORK/np-acl.toml"
todo 'no <roles> section was found'  "$WORK/np-acl.toml"
todo 'NO RULE was written into this file' "$WORK/np-acl.toml"
# ...and an unreadable credentials file puts the gap in the CONFIG too, not just stderr.
python3 "$CONV" "$FIX/hivemq-2026.5-config.xml" --credentials "$WORK/does-not-exist.xml" \
  --out-config "$WORK/missing.toml" --out-acl "$WORK/missing-acl.toml" >/dev/null 2>&1 \
  || fail "an unreadable credentials file should not be fatal"
todo 'THE AUTHORIZATION POLICY WAS NOT TRANSLATED' "$WORK/missing.toml"
[[ -s "$WORK/missing-acl.toml" ]] \
  || fail "an unreadable credentials file produced NO acl.toml while the config names one"
todo 'NOTHING WAS TRANSLATED INTO THIS FILE' "$WORK/missing-acl.toml"
ok "a policy-less and an unreadable credentials file both report the gap in both files"

# ── 8e. the two VERBATIM vendor files, unmodified ──────────────────────────────────────
# hivemq-2026.5-config.xml is a MERGE of vendor parts plus two blocks written here from
# the XSD, and its header says so — necessary, because HiveMQ ships <listeners>, <mqtt>
# and <restrictions> in separate sample files. These two fixtures are single vendor files
# byte for byte (SHA-256s in their headers), so they answer the question the merged one
# cannot: does an actual shipped HiveMQ file convert?
#
# (a) src/main/resources/config.xml — the config.xml a stock CE install has. One plaintext
#     listener, telemetry on, and NO auth of any kind, because CE has none: the conversion
#     must say the deployment was anonymous rather than emit a config that looks finished.
python3 "$CONV" "$FIX/hivemq-2026.5-default-config.xml" \
  --out-config "$WORK/vdefault.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the VERBATIM vendor default config.xml"
"$MQTTD_BIN" --check-config --config "$WORK/vdefault.toml" >/dev/null 2>"$WORK/vdefault.err" \
  || { echo "  FAIL — the broker REJECTED the VERBATIM vendor default config.xml's output:";
       sed 's/^/         /' "$WORK/vdefault.err"; exit 1; }
todo 'every client was ANONYMOUS' "$WORK/vdefault.toml"
grep -q 'allow_anonymous = false' "$WORK/vdefault.toml" \
  || fail "the anonymous CE config did not come out refusing anonymous clients"
grep -qE '^client_ca' "$WORK/vdefault.toml" \
  && fail "a config.xml with no <tls> at all produced a client_ca"
# This is the only fixture whose output has NO transport security whatsoever: a stock CE
# install is one plaintext listener, so a silent conversion hands the operator a broker
# with credentials crossing in the clear and nothing in the file to say so.
grep -q 'a PLAINTEXT listener was carried over' "$WORK/vdefault.toml" \
  || fail "the stock CE config's plaintext-only listener was carried over with no warning"
grep -qE '^\[tls\]' "$WORK/vdefault.toml" \
  && fail "a config.xml with no TLS material produced a [tls] table"

# (b) tls/config-sample-mqtt-tls-client-auth.xml — the vendor's OWN mTLS example, and the
#     one shipped file that takes the POSITIVE branch of the client-authentication-mode
#     mapping. Without it, that mapping would only ever be tested on input written here.
python3 "$CONV" "$FIX/hivemq-2026.5-tls-client-auth.xml" \
  --out-config "$WORK/vtls.toml" >/dev/null 2>&1 \
  || fail "the converter failed on the VERBATIM vendor mTLS example"
"$MQTTD_BIN" --check-config --config "$WORK/vtls.toml" >/dev/null 2>"$WORK/vtls.err" \
  || { echo "  FAIL — the broker REJECTED the VERBATIM vendor mTLS example's output:";
       sed 's/^/         /' "$WORK/vtls.err"; exit 1; }
grep -qE '^client_ca' "$WORK/vtls.toml" \
  || fail "the vendor's own REQUIRED client-authentication-mode did not map to client_ca"
grep -q 'clientAuth extended key usage' "$WORK/vtls.toml" \
  || fail "the mTLS mapping did not warn about the clientAuth EKU requirement"
# The keystore and truststore PASSWORDS the vendor's example carries must never travel.
for secret in password-keystore password-key password-truststore; do
  grep -q "$secret" "$WORK/vtls.toml" \
    && fail "the vendor example's keystore password '$secret' was copied into the output"
done
ok "both VERBATIM vendor files convert, validate, and behave (anonymous CE reported; REQUIRED maps; passwords never copied)"

# ── 9. degenerate inputs ─────────────────────────────────────────────────────────────
printf '<hivemq></hivemq>\n' > "$WORK/empty.xml"
python3 "$CONV" "$WORK/empty.xml" --out-config "$WORK/empty.toml" >/dev/null 2>&1 \
  || fail "an EMPTY <hivemq> document crashed the converter"
todo 'NO listener was found' "$WORK/empty.toml"
"$MQTTD_BIN" --check-config --config "$WORK/empty.toml" >/dev/null 2>&1 \
  || fail "the empty-document config does not pass --check-config"
printf '<hivemq><listeners><tcp-listener><port>1883\n' > "$WORK/broken.xml"
python3 "$CONV" "$WORK/broken.xml" --out-config "$WORK/broken.toml" >/dev/null 2>&1 \
  || fail "MALFORMED XML crashed the converter (it must report, not raise)"
todo 'NOT parseable XML' "$WORK/broken.toml"
if python3 "$CONV" "$WORK/does-not-exist.xml" >/dev/null 2>&1; then
  fail "an unreadable input should exit 1, not 0"
fi
ok "an empty document and malformed XML both report themselves (exit 0); a missing file exits 1"

# ── 10. THE assertion: the real broker boots on the converted ACL ────────────────────
# Run twice: once on the vendor fixture's policy, and once on the hostile-strings policy —
# a backslash identity and a double-quoted topic filter have to survive the broker's own
# ACL loader, not only tomllib.
boot_on_acl() {  # $1 = acl file, $2 = what it proves
  local acl="$1" what="$2" port hport
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
  if ! curl -fsS "http://127.0.0.1:$hport/readyz" >/dev/null 2>&1; then
    echo "  FAIL — the broker REJECTED $what:"
    tail -5 "$WORK/boot.log" | sed 's/^/         /'
    exit 1
  fi
  kill $BROKER 2>/dev/null || true
  wait $BROKER 2>/dev/null || true
  ok "the broker booted on $what"
}
boot_on_acl "$WORK/acl.toml" "the converted ACL"
boot_on_acl "$WORK/hostile-acl.toml" \
  "the ACL holding a backslash identity and a quoted topic filter"
# ── a bind mqttd cannot bind, and an identity it cannot express ─────────────────────────
#
# `mqttd --check-config` accepts ANY string in a bind (resolution happens at bind time), so the
# verification this converter's header, --help and docs/MIGRATION.md point the operator at
# verified NOTHING about the one value the provenance restructuring is about: `<port>abc</port>`
# produced a live `plaintext_bind = "10.0.0.1:abc"`, `config OK`, and then `invalid port value`
# at startup — at the maintenance window. And a file-RBAC `<name>` containing a literal `*`
# became an mqttd identity GLOB, which has no escape (crates/mqtt-auth/src/acl.rs), so the rule
# would apply to every identity matching the pattern while HiveMQ matched the name exactly.
cat > "$WORK/badport.xml" <<'XML'
<hivemq><listeners><tcp-listener><port>abc</port><bind-address>10.0.0.1</bind-address>
</tcp-listener></listeners></hivemq>
XML
cat > "$WORK/star-credentials.xml" <<'XML'
<file-rbac><users><user><name>alice*bob</name><password>x</password>
<roles><id>r1</id></roles></user></users>
<roles><role><id>r1</id><permissions><permission><topic>out/#</topic></permission>
</permissions></role></roles></file-rbac>
XML
python3 "$CONV" "$WORK/badport.xml" --credentials "$WORK/star-credentials.xml" \
  --out-config "$WORK/badport.toml" --out-acl "$WORK/star-acl.toml" >/dev/null
if grep -qE '^plaintext_bind = ' "$WORK/badport.toml"; then
  fail "an address the broker cannot bind was emitted LIVE (check-config would still say OK)"
fi
grep -qF 'not a TCP port number' "$WORK/badport.toml" \
  || fail "the unbindable port was not reported"
grep -qF '10.0.0.1' "$WORK/badport.toml" \
  || fail "the bind-address the input named is nowhere in the output"
if grep -q 'identities = \["alice\*bob"\]' "$WORK/star-acl.toml"; then
  fail "a LITERAL '*' in a file-RBAC user name became an mqttd identity GLOB"
fi
grep -qF 'alice*bob' "$WORK/star-acl.toml" \
  || fail "the refused user name is not named anywhere in the ACL"
"$MQTTD_BIN" --check-config --config "$WORK/badport.toml" >/dev/null 2>&1 \
  || fail "the broker rejected the config built from an unbindable listener port"
ok "an unbindable port and a glob-metacharacter user name are both refused and named"

# ── the PROPERTY SWEEP ──────────────────────────────────────────────────────────────────
# Everything above is example-based: one input, a list of greps. That shape catches a
# regression exactly where a reviewer already looked and is blind everywhere else — which is
# how the same first-listener-only TLS defect was found three times in three converters, each
# harness having only ever fed its converter ONE ordering of ONE listener set. The sweep
# generates many inputs (listener ORDER, enable flags, mTLS postures, no_match postures,
# truststore presence) and asserts one invariant per defect CLASS on every one of them:
# nothing silently dropped, nothing disabled activated, no claim contradicting the value it
# describes, no dangling `step N`, and `--check-config` on every generated config.
python3 scripts/migrate/property_sweep.py hivemq --mqttd "$MQTTD_BIN" \
  || fail "the hivemq property sweep found a case the fixture tests cannot see"

# ── the FUZZ pass ───────────────────────────────────────────────────────────────────────
# The generators above enumerate axes their author thought of, which is how round 2's
# blocking defect survived round 1. This pass does not think: it mutates each fixture
# mechanically (delete lines, truncate mid-structure, permute blocks, flip enable flags, swap
# transports) and asserts only what must hold for ANY byte sequence — the converter EXITS,
# 0 or 1, with a message; whatever it writes is valid TOML; and no live security-relevant
# line lacks provenance. It is how the HOCON reader's infinite loop was found (a two-line
# input: `authentication = [` then `}`), which no example-based test could have produced.
python3 scripts/migrate/property_sweep.py hivemq --fuzz 40 \
  || { echo "  FAIL — the hivemq fuzz pass found an input this converter wedges, crashes or invents on";
       exit 1; }

echo "HIVEMQ MIGRATE OK"
