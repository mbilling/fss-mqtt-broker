# Migrating onto mqttd

**Dated 2026-08-15.** Two halves, and you need both:

1. **Convert the configuration.** [`scripts/migrate/`](../scripts/migrate/) has a
   converter per source broker. Each one translates what maps and marks everything
   else `TODO(migrate)` in the file you are about to deploy.
2. **Move the traffic.** mqttd cannot import another broker's **session** state, so
   cutover is a *dual run*: bridge both brokers, move clients in cohorts, verify,
   then cut. That is the [second half](#the-dual-run-cutover) of this page, and it is
   not optional.

Converters are **best-effort by declared design** — a bounded common-subset mapping
with an honest failure mode, not a compatibility promise
([ADR 0051](adr/0051-evaluation-readiness.md) §3). They exist because
hand-translating an ACL file with one entry per device is not a task anyone will do
for an evaluation, so the evaluation does not happen.

---

## What a converter produces: a DRAFT, where anything undecidable is INERT and named

This is the promise, and it is deliberately narrower than "your config, translated".

Four adversarial review rounds each fixed what they were shown, honestly, and the finding
count went **up** each time. The reason is structural rather than a failure of effort: "every
input construct is either translated or marked `TODO(migrate)`" is a **total-coverage claim
over three large foreign vendor schemas**, and total coverage cannot be reached by
inspection. But rounds 1–3 all pointed one way — **the tool emitted a live setting it had not
actually derived from the input**:

- a bind fabricated as `0.0.0.0:1883` when the input said `bind_address 127.0.0.1` (a
  loopback-only broker published on every interface, on a port nobody wrote);
- a listener EMQX had switched **off** converted into a live bind;
- `allow_anonymous true` from a retired listener carried onto the live one;
- an mTLS mandate, a CRL check and a TLS-version floor dropped from any listener that was not
  first in document order;
- an ACL file that permits everything while its own comment says it denies everything;
- a WebSocket listener emitted as a raw-MQTT bind.

### What the gate does guarantee, exactly

Every security-relevant value each converter writes — every `*_bind`, every path under
`[tls]`, `client_ca`, `crl`, `acl_file`, `password_file`, `allow_anonymous`,
`mtls_identity_source`, every `[security.*]` value, the ACL `default`, and every bridge
upstream `url` and its TLS block — is emitted through **one gate** that takes the value *and
the input key it was derived from*, and **refuses to write a live line without one**. A value
with no provenance comes out **commented out**, beside a `TODO(migrate)` naming the decision
the operator has to make.

So the guarantee is exactly this, and it is worth stating in one sentence because it is
smaller than it sounds: **no live security-relevant value can be emitted without naming the
input key it came from**, which makes a *fabricated* value structurally impossible and every
live one checkable against your own file. Invariants **F** and **G** below hold it there over
generated and mechanically mutated inputs, and the first time they ran they caught a fabricated
bind that a fix written three hours earlier had introduced.

### What it does NOT guarantee: MISREADING a real input

The gate proves a live value came from a named input key. It cannot prove that the key **means
what the converter took it to mean** — and round 4 found that class in five places, each one a
value genuinely derived from real input, each one carrying an honest `# from:`:

- a Mosquitto **TLS-PSK** listener (`psk_hint`, which mosquitto.conf(5) says "enables
  pre-shared-key support for this listener") became a **live plaintext bind** on the same port,
  while another TODO in the same file reported that listener's `tls_version`. The bind carried
  a real `# from: listener 8883`; the gate had nothing to object to, because it checks where the
  *value* came from and the **field** is what encodes the transport;
- Mosquitto's **anonymous-scoped ACL block** — `topic` lines before the first `user` line,
  which the man page scopes to anonymous clients only — became rules with no `identities`,
  which mqttd applies to **every authenticated client**;
- an EMQX bridge whose `ssl.enable = true` kept a **live upstream `url`** beside a commented
  `[upstreams.tls]`, so completing the draft exactly as the file instructs produced a bridge
  that connected to a TLS peer in cleartext;
- `message_size_limit 0` — the vendor's documented spelling of *no limit* — became
  `max_packet_size = 0`, which mqttd floors to 1024, turning an unlimited broker into one that
  refuses any packet over 1 KiB;
- a username containing a literal `*` became an mqttd identity **glob**, granting a rule to
  every identity matching the pattern.

All five are fixed, each pinned by a test named in
[`docs/delivery/0051-evaluation-readiness.md`](delivery/0051-evaluation-readiness.md). But the
class is **unbounded across three vendor schemas**, and no invariant over the output can close
it: the output is consistent with the input in every one of these cases. The honest statement
is therefore that the gate closes **fabrication** and not **misreading**, and that the
misreadings known today are enumerated in [KNOWN GAPS](#known-gaps-after-round-4) rather than
implied to be absent.

The visible consequence is that every live security-relevant line carries where it came from:

```toml
[listeners]
tls_bind = "127.0.0.77:18883"  # from: port 18883 + bind_address 127.0.0.77
plaintext_bind = "0.0.0.0:1883"  # from: listener 1883; defaulted: the host, because that directive named no address and mosquitto.conf(5) then listens on EVERY interface

[tls]
cert = "/certs/device.crt"  # from: certfile at listener 0.0.0.0:8883
```

and that a construct the converter failed to read leaves the config **incomplete** rather
than live:

```toml
# TODO(migrate): a tls listener was configured (see the settings attached to it) but the
# input gave its address as `bind_address 10.0.0.5` but NEVER a port. mosquitto.conf(5)
# documents the default as 1883 — that is a default of the BROKER, not a value in your file
# […] The commented line below is a PLACEHOLDER, not a value from your config
# tls_bind = "10.0.0.5:1883"
```

**The worst case a FABRICATION can produce is therefore a config you have to finish.** That is
the difference this page is asking you to trust, and it is checkable by eye
(`grep -c '# from:'`) and by machine (invariants **F**, **G** and **H** below, plus
`--provenance-json` on any converter, which writes the same ledger as JSON). The worst case a
MISREADING can produce is a wrong live value with an honest source on it — which is why the
[KNOWN GAPS](#known-gaps-after-round-4) list exists and why the output still has to be read.

### Verified, and not verified

**Verified:**

- every fixture's provenance re-derived by re-fetching the vendor file and diffing (SHA-256
  for the verbatim ones — [the provenance section](#honesty-about-provenance) has the
  commands);
- the eight invariants of `scripts/migrate/property_sweep.py` over 138 generated inputs
  (40 Mosquitto / 57 EMQX / 41 HiveMQ), each one mutation-proved;
- for **Mosquitto only**: a **differential lane** against the real vendor broker
  (`scripts/migrate/differential-mosquitto.sh`) — the source config booted on real
  `mosquitto`, the converted config booted on `mqttd`, and seven observable verdicts
  compared (anonymous access, a wrong password, a valid credential, a permitted and a
  forbidden publish, a permitted and a forbidden subscription). This is the only check on
  this page that tests a mapping against the vendor's **behaviour** rather than our reading
  of its documentation; it covers exactly the auth/ACL/bind mappings its seven probes
  touch, and nothing else;
- a **fuzz pass** (`--fuzz N`) over mechanically mutated fixtures — random lines deleted, the
  file truncated mid-structure, listener blocks permuted, `enable` flags flipped, transports
  swapped — asserting only that the converter always exits 0 or 1 **with a message**, never
  hangs, writes valid TOML, and never emits a live security-relevant line without provenance;
- `mqttd --check-config` on **every** generated config, and the translated ACL loaded by a
  real booted broker, in every converter's fixture test;
- the Mosquitto converter and its Rust twin compared **byte for byte** on shared fixtures.

**Not verified, and no amount of the above changes it:**

- **No live EMQX or HiveMQ broker has ever been run** against these converters, and no
  ground-truth config produced by one was used. Mosquitto's mappings likewise come from
  `mosquitto.conf(5)` @ `v2.0.22`, not from a running broker — except for what the
  differential lane above measures: seven behavioural verdicts on one auth+ACL config,
  which is a probe, not coverage. Every Mosquitto mapping outside those probes is exactly
  as unverified as the other two brokers'.
- **No claim of total coverage over any vendor's schema is made.** A construct a converter
  has never seen is a construct it cannot report. What the restructuring above buys is that
  such a construct cannot produce a *live* security setting either — it can only leave a hole.
- **No claim that a construct a converter DOES read is read CORRECTLY.** The provenance gate
  proves a live value came from a named input key, not that the key means what the converter
  took it to mean; round 4 found five misreadings, all fixed, and the class is open. Every one
  known today is in [KNOWN GAPS](#known-gaps-after-round-4), with what to check by hand.
- HiveMQ **Enterprise**'s schema is not open source; only CE's `config.xsd` was read.

Each converter's `--help` says the same thing, and so does the header of every file they
write.

---

## What ships

| Source broker | Converter | Config | ACL / RBAC | Bridges | Fixture test |
|---|---|---|---|---|---|
| Mosquitto 2.x | `from-mosquitto.py` (+ `mqttui migrate mosquitto`) | ✔ | ✔ `acl_file` | ✖ (reported) | `test-from-mosquitto.sh` |
| EMQX 5.x / 6.x | `from-emqx.py` | ✔ HOCON | ✔ `acl.conf` Erlang terms | ✔ MQTT bridges → `mqtt-bridge` | `test-from-emqx.sh` |
| HiveMQ CE 2023.x–2026.x | `from-hivemq.py` | ✔ `config.xml` | ✔ File RBAC `credentials.xml` | ✖ (reported) | `test-from-hivemq.sh` |
| NanoMQ | — | not written yet (0051-T8) | — | — | — |
| VerneMQ | — | not written yet, not tracked | — | — | — |

The version ranges in that first column are what the *parsers* accept, not a range anything
is pinned to. **Two of the three brokers have a vendor config file pinned as a fixture and
running in CI (EMQX 6.2.2, HiveMQ CE 2026.5 + file-RBAC 4.6.16, each with a re-derivable
SHA-256); Mosquitto has NONE** — its row says "Mosquitto 2.x" because that is the release its
mappings were written against from `mosquitto.conf(5)`, and `test-from-mosquitto.sh` writes its
inputs inline. The neighbouring versions were spot-checked by hand —
[read the exact scope before relying on it](#honesty-about-provenance).

### KNOWN GAPS (after round 4)

Every construct known, as of **2026-08-17**, to be **unhandled** or **misread** by a converter,
with what the operator must check by hand. This list exists because the provenance gate closes
fabrication and not misreading (see [above](#what-it-does-not-guarantee-misreading-a-real-input)):
a construct in this table can still produce a wrong *live* value with an honest `# from:` on it.
It is deliberately a list of named holes rather than a claim of completeness — the ones that are
merely *unknown* are not in any table, which is the point of the "no total coverage" statement.

| Construct | Converter | What happens now | What you must check by hand |
|---|---|---|---|
| **Mosquitto TLS-PSK listener** (`psk_hint` / `psk_file`) | Mosquitto (+ `mqttui`) | **Not translated.** The listener is *encrypted* and mqttd has no PSK at all, so its bind is emitted **inert on the TLS key** with a TODO, and `psk_file`'s identities are named. Never a plaintext bind | Every PSK device needs a new credential — a certificate CN, or an Argon2id password entry — and the ACL must key on whichever you choose. Until then those clients cannot connect |
| **Mosquitto anonymous-scoped ACL block** (`topic` lines before the first `user`) | Mosquitto (+ `mqttui`) | Emitted **scoped** to `identities = ["anonymous"]`, the subject mqttd gives an unauthenticated client, with a TODO quoting the man page | Those rules grant nothing until `allow_anonymous` is uncommented (mqttd refuses anonymous by default). If `allow_anonymous` was false in Mosquitto they were already dead — delete them. A real user literally named `anonymous` collides |
| **A literal `*` in a username** (any source) | all three | **No rule emitted**, with a TODO naming the user. mqttd's `identities` are globs with no escape, so the rule cannot be expressed without widening it | Rename the user, or write rules enumerating the exact identities. The rule is *missing* until you do — deny-by-default means those clients lose access |
| **A literal `%c`/`%i` in a topic** (a Mosquitto plain `topic` filter, an EMQX `acl.conf` `topic_match()`, a HiveMQ file-RBAC `<topic>`) | all three | **No rule emitted**, with a TODO naming the topic. Every source matches those bytes literally (Mosquitto substitutes only in `pattern`; EMQX 5/6 substitutes only `${...}` placeholders; file-RBAC substitutes only `${{clientid}}`/`${{username}}`), while mqttd substitutes in every rule and has no escape. Until issue #297 the EMQX and HiveMQ converters emitted these as substituting rules | Decide which you meant. A per-client namespace is expressible deliberately (and fails closed on a value containing `/`, `+` or `#`); a literally-named topic is not. An EMQX file carrying `%c`/`%u` as *placeholders* is an EMQX 4.x file — outside the parser's version scope; rewrite them as `${clientid}`/`${username}` |
| **Mosquitto bridge blocks** (`connection`, `address`, `topic`, `bridge_*`, `remote_*`) | Mosquitto (+ `mqttui`) | **Reported per key, each naming its `mqtt-bridge` equivalent.** No bridge config is written — this converter has no `--out-bridge` | Write the `mqtt-bridge` config by hand from those TODOs ([docs/BRIDGE.md](BRIDGE.md)). `[upstreams.tls]` is optional and **absent means plaintext**, so an omitted `ca` silently drops peer verification |
| **Mosquitto `include_dir`, `plugin`, `plugin_opt_*`** | Mosquitto (+ `mqttui`) | **Contents never read**, reported in those words. A Dynamic Security deployment keeps its whole user/role/ACL policy in a JSON file the converter never opens | Concatenate the included `.conf` files and re-run; export and re-model the dynsec policy. Nothing from either is in the output, and what was never seen cannot be reported |
| **`message_size_limit` → `max_packet_size`** | Mosquitto (+ `mqttui`) | Mapped, with a NOTE: it is the **nearest** equivalent, not the same quantity (payload vs whole packet). `0` (the vendor's *no limit*) leaves the key unset | If you publish near the limit, raise the cap by your largest topic + MQTT 5 property overhead, or publishes Mosquitto accepted are refused |
| **EMQX authenticators on a non-`http`/non-`jwt` backend** (`mysql`, `redis`, `postgres`, `mongodb`, `ldap`, `scram`, unknown mechanisms) | EMQX | **Not translated**, and every key is now named — the server, database, query/`cmd` and hash algorithm | Rebuild the check behind `[security.http_auth]` (one HTTP hook, status code is the verdict) or re-enrol users into an Argon2id file. Nothing authenticates those clients until you do |
| **EMQX `acl_claim_name`** (authorization delivered inside the JWT) | EMQX | **Not translated**, reported in full: mqttd reads no rules from a token claim | Re-model as OIDC `groups` + `groups = [...]` ACL rules, or keep EMQX for those clients. After cutover they are governed *only* by the file policy — locked out where the token granted more, over-permitted where it granted less |
| **EMQX `verify_claims` other than `iss`/`aud`** | EMQX | `iss`/`aud` map onto `[security.jwt]`; every other claim constraint is a TODO | A token satisfying only the signature now passes. Move the check into `[security.http_auth]`, or narrow the issuer |
| **EMQX bridge payload/retain templates, `${...}` topic rewrites** | EMQX | Per-key TODOs; `mqtt-bridge` forwards the payload byte for byte and can only strip/prepend a topic prefix | Anything that reshaped a message must move into a client. A rewrite that is not a prefix pair is **not** applied — the rule forwards the topic unchanged |
| **EMQX bridge with `ssl.enable = true`** | EMQX | The upstream `url` **and** `[upstreams.tls]` are both inert, with the reason | Fill in `[upstreams.tls]` with paths that exist where `mqtt-bridge` runs, *then* uncomment the `url`. Uncommenting the `url` alone connects in cleartext |
| **HiveMQ keystore/truststore (JKS)** | HiveMQ | Not readable here: the PEM paths are emitted as `defaulted:` candidates with an extraction recipe | Run the recipe (`keytool` + `openssl`), then point `[tls] cert`/`key` at what it produced. The paths in the output are *yours to create*, not paths that exist |
| **HiveMQ `<qos>` / `<retain>` / `<shared-subscription>` permission qualifiers** | HiveMQ | The rule is emitted **covering every QoS / retain flag / subscription kind**, with a TODO saying so — this is the *broader* direction | Re-check each qualified permission by hand; mqttd's ACL has no such qualifier, so a narrow HiveMQ permission becomes a wider mqttd one |
| **HiveMQ Enterprise** | HiveMQ | Only CE's `config.xsd` was read; Enterprise's schema is not open source | Any Enterprise-only element is unrecognised — it will be reported by path, but no mapping for it exists |
| **A non-UTF-8 config** | all three | Exit 1 with a message naming the encoding problem (it used to be a traceback) | Re-save as UTF-8 (`iconv`) and re-run |
| **`${ENV_VAR}` interpolation in an EMQX `bind`** | EMQX | Reported inert (there is no port), but the TODO prints the value with its closing `}` lost — a cosmetic defect in the HOCON reader's bare-value scanner | Read the value from your own file, not from the TODO |
| **Whatever nobody has looked at yet** | all three | Unknown keys are reported by path; unknown *meanings* are not detectable at all | This is the honest end of the list: a construct the converter reads but misunderstands can still produce a live value, and only reading the output against your own config finds it |

**Three brokers are supported; every other broker is the manual path, and it is not
cheap.** There is no converter for NanoMQ, VerneMQ, AWS IoT Core, Azure IoT Hub, RabbitMQ's
MQTT plugin, ActiveMQ, or anything else — and no partial one that half-reads their files.
What the manual path costs, so you can price it before committing:

- **The config itself is the small part.** `docs/mqttd.example.toml` is annotated
  end to end and `mqttd --check-config` tells you immediately whether a hand-written file
  is valid, so a single node's listeners, TLS paths and limits are an hour's work.
- **The ACL is the expensive part**, and it scales with your fleet rather than your
  cleverness: one entry per device, re-modelled from first-match-wins ordering (which
  almost every broker uses) onto mqttd's deny-wins set semantics. That re-modelling is
  where a hand translation silently becomes *more permissive* — the same trap the
  [EMQX ACL section](#the-acl-and-the-ways-a-conversion-can-be-more-permissive)
  describes, minus the tool that warns you. This is the whole reason the converters exist.
- **Credentials cannot be carried over at all**, converter or not. Every scheme's hashes
  are unconvertible, so it is `mqttd --hash-password` per user regardless of source broker.
- **The dual-run cutover is broker-agnostic.** The [second half](#the-dual-run-cutover)
  of this page needs nothing from a converter — `mqtt-bridge` speaks plain MQTT to both
  sides — so an unsupported incumbent loses you the config translation and none of the
  traffic-migration story.

If your broker is not in the table, the honest read is: the cutover playbook applies
unchanged, and the config work is yours. Nothing here pretends otherwise.

Only the Mosquitto converter has a Rust twin inside `mqttui` (ADR 0056 D, with a
byte-for-byte differential test). **The EMQX and HiveMQ converters are Python 3
only** — `python3` is already a repo tool dependency, but if you have no Python you
cannot run them, and there is no parity claim here to the contrary.

### The property that makes them reviewable

**Every construct a converter READS is either translated or becomes a `TODO(migrate)` line —
never a silent drop.**

Note the qualifier, which the rest of this page earns: it is a claim about what the converter
read, not about your whole file. A construct it has never seen cannot be reported (there is no
total-coverage claim over any vendor schema), and a construct it reads but *misunderstands* is
reported as translated — see [KNOWN GAPS](#known-gaps-after-round-4). Within that bound the
property is real and worth having: a converter that quietly loses an ACL rule is worse than no
converter, because you ship a broker missing a rule you believe you migrated. So every unmapped
setting — including constructs mqttd has no equivalent for *at all* — comes out as a comment
**at the point it belongs**, often with a commented-out candidate line beside it:

```toml
# TODO(migrate): cacertfile was set but client certificates were NOT mandatory
# (verify = verify_none, fail_if_no_peer_cert = false). mqttd's client_ca MANDATES
# mTLS — there is no cert-optional mode. Uncomment to require certificates
# fleet-wide, or leave it commented for server-only TLS:
# client_ca = "/etc/emqx/certs/ca.pem"
```

Three rules, from ADR 0051 §3, and all three are asserted by the fixture tests — the first one
scoped, again, to what the converter read:

- **No construct a converter reads is silently dropped.** An unknown key becomes a TODO naming
  its path. What is *not* claimed: that every construct in your file is read (nothing can claim
  that over a foreign schema), or that a construct which IS read is understood — round 4 found
  five misreadings and [KNOWN GAPS](#known-gaps-after-round-4) lists them.
- **Secrets are never transformed.** Password hashes cannot be converted between
  schemes, so the converters do not try — and they never copy key material,
  keystore passwords or inline secrets into their output either. They emit a
  per-user `mqttd --hash-password` re-enrolment list instead.
- **The output must validate.** Every converted config in the fixture tests is put
  through `mqttd --check-config`, and the translated ACL is loaded by a real booted
  broker.

#### And a property sweep, because three fixture tests were not enough

The fixture tests are **example-based**: one realistic input each, a list of greps. That
shape catches a regression exactly where a reviewer already looked and is blind everywhere
else — which is how the *same* defect (TLS read off the first listener and applied as if it
were global) was found three separate times in three converters. Each harness had only ever
fed its converter **one ordering of one listener set**, so the mandate on a second listener
was invisible to all three.

So `scripts/migrate/property_sweep.py` generates inputs from a cross product — listener
**order** permutations, `enable` flags, mixed and unanimous mTLS postures, both `no_match`
postures, truststore present and absent, the Mosquitto **default-listener** form
(`port`/`bind_address`), `protocol websockets`, a listener-scoped EMQX authentication chain,
the other keys on a *live* authenticator, `acl_claim_name`, a TLS bridge (`--out-bridge`), and
per-listener `max_connections`/`messages_rate`, a Mosquitto **TLS-PSK** listener, a UNIX-socket
listener, an **anonymous-scoped** ACL block, glob-metacharacter usernames, a non-scalar and a
host-less EMQX `bind`, an authenticator on a backend mqttd has no equivalent for, and
`verify_claims` — and asserts one invariant per defect **class** on every generated case. All
three `test-from-*.sh` scripts run it, so it is CI-gated:

| Invariant | What it makes detectable |
|---|---|
| **A** every security-relevant input **value** appears in the output, translated or named in a `TODO`/`NOTE` | a per-listener (or per-source, per-user) setting read at index 0 and reported as if it covered the file |
| **B** a construct the source **disabled** is never a live bind, URL or `[[rules]]` entry — and is still named | a listener, authenticator, authz source or user switched back ON by the conversion |
| **C** no `deny`/`allow` claim in a document contradicts the `default` that same document writes | a comment asserting "fail-closed" inside a wide-open policy |
| **D** every `step N` the output tells you to run is a step the output printed | a `client_ca` placeholder whose extraction recipe was never emitted |
| **E** `mqttd --check-config` on **every** generated config | a combination the broker rejects that no single fixture happens to produce |
| **F** *(2026-08-15)* **PROVENANCE**: every live security-relevant value is one the **input held** — for a `*_bind` the **port** must be a port the input named, for a path or URL the string must appear in the input | a **fabricated** bind or path. This is the one A could not see: A asks whether a value appears *somewhere*, so it passed while `tls_bind` was invented as `0.0.0.0:1883` for an input that said `port 18883` — the input's own values *did* appear, inside a TODO that misdescribed them |
| **G** *(2026-08-15)* **NO LIVE SETTING WITHOUT A SOURCE**: every uncommented security-relevant line carries `# from: <input key>` | the whole class, structurally: it is what proves the emission gate was not bypassed at a site nobody has looked at yet |
| **H** *(2026-08-15)* **EVERY LIVE BIND IS BINDABLE**: the host and port of every live `*_bind` are parsed | a bind `--check-config` accepts and the broker then refuses at STARTUP — `ws_bind = ":8085"` (a form EMQX's own `ip_port` accepts), `plaintext_bind = "10.0.0.1:abc"`, and a Mosquitto UNIX-socket listener turned into `"/tmp/mosq.sock:0"`. **E could not see any of them**, which is why the verification every `--help` and every generated header points the operator at did not cover the one value this table is about |

A value part the input did not hold is allowed under **F** only where the line says
`defaulted: <what, and the vendor default it came from>` — a host for a listener directive
that named no address, or a path the converter itself owns (`/etc/mqttd/acl.toml`, and the PEM
paths HiveMQ's keystore has to be *extracted* into). Every one of those is **counted and
printed** by the sweep, so the escape hatch cannot be used silently, and the port half of a
bind may never use it.

Each invariant is mutation-proved: reverting the corresponding fix makes the sweep fail with
a named message, and the table in
[`docs/delivery/0051-evaluation-readiness.md`](delivery/0051-evaluation-readiness.md) records
which mutation each one catches. **What F and G caught the first time they ran**, before the
restructuring they were written for: G fired on *every* `[listeners]`, `[tls]` and `[security]`
line of the EMQX and HiveMQ output (none carried a source), F caught `acl_file` pointing at a
path that appears nowhere in any input, and — once the same rule was applied inside the fuzz
pass — F caught a **live `plaintext_bind` on port 1883 derived from a `bind_address` with no
port**, in the Mosquitto converter, in a fix written earlier the same day. That last one is
the whole argument for the invariant: the mistake was mine, it was three hours old, and
nothing but a machine reading the output against the input would have found it.

#### And a fuzz pass, because a converter must never wedge

`property_sweep.py <converter> --fuzz N` mutates each pinned fixture mechanically — delete
random lines, truncate at a random byte, permute blocks, flip a boolean or an enum, swap a
transport, duplicate or chop a line — and asserts only what must hold for **any** byte
sequence: the converter exits **0 or 1 with a message** (the documented contract), whatever it
writes is valid TOML, and no live security-relevant line lacks provenance. Four seeded
reproducers run unmutated on every pass.

It found a **hang**: `from-emqx.py`'s hand-written HOCON reader spun at 100% CPU with
unbounded memory growth, forever, on a **two-line** input — `authentication = [` followed by
`}` — because its bare-value scanner could not advance past a `}` it had already reached, so
it appended an empty item and re-entered the loop unchanged. In CI or a `mqttui migrate` that
is a wedge plus an OOM rather than a diagnosable failure, and EMQX itself reports a syntax
error on the same file. The fix is structural, not a special case: every loop in that reader
now refuses to iterate without consuming a character, and an unterminated `[`/`{` is reported
as a TODO saying that everything after the missing bracket was read in the **wrong scope**.
2,300+ mutated inputs across six seeds are clean of hangs, crashes and invalid TOML.

**What the mutator still cannot generate, stated because the invariant is otherwise
over-claimed:** it mutates the *decoded* text of each fixture, so it can never introduce a raw
control byte or invalid UTF-8. Both of those classes were found by hand instead and both are
fixed — a control character inside a value used to survive into a TOML comment (TOML 1.0 forbids
one anywhere in a document, so `tomllib` and the broker rejected the whole file while the
converter printed `wrote <file>`; it is now escaped as `\uXXXX`), and a non-UTF-8 config used to
exit 1 on a bare `UnicodeDecodeError` traceback instead of the documented message. Three
**seeded** reproducers now carry a control character through each converter on every fuzz run, so
the class is guarded even though the mutator cannot reach it; mutating BYTES rather than decoded
text is the follow-up that would let the fuzz find such a thing itself.

### Honesty about provenance

I did not run EMQX or HiveMQ. The fixtures come in **three kinds, and the distinction is
the point** — an earlier version of this page called composed fixtures "verbatim", which
is precisely the sort of claim this section exists to make checkable. Each fixture's own
header now carries its provenance, and for the verbatim ones a **SHA-256 you can re-derive
with `curl` + `shasum -a 256`**.

**1. Verbatim vendor files.** Byte for byte what the vendor ships, header comment aside.
They answer one question: *does an actual shipped config convert?*

| Fixture | Source, at a pinned tag | What it proves |
|---|---|---|
| `fixtures/emqx-6.2.2-vendor-verbatim.conf` | `emqx/emqx` @ `6.2.2` — ten `rel/config/examples/*.conf.example` files concatenated, unmodified | a stock EMQX file converts, passes `--check-config`, and takes the **refusing** branch of every security mapping: `verify_none` on all three TLS listeners does **not** become an mTLS mandate, `max_retained_messages = 0` is read as *unlimited* rather than a cap of zero, and all three TLS listeners are named |
| `fixtures/emqx-acl-6.2.2.conf` | `emqx/emqx` @ `6.2.2`, `apps/emqx_auth/etc/acl.conf` | the shipped default ACL, whose four rules each land on a different gap: **six** `TODO(migrate)` lines and **one** emitted rule (the `$SYS` deny, kept and reported as inert) |
| `fixtures/hivemq-2026.5-default-config.xml` | `hivemq/hivemq-community-edition` @ `2026.5`, `src/main/resources/config.xml` | the config.xml a stock CE install actually has — one plaintext listener and no auth at all. The conversion must say the deployment was **anonymous** and must warn that the plaintext listener carried over |
| `fixtures/hivemq-2026.5-tls-client-auth.xml` | same repo/tag, `src/distribution/conf/examples/configuration/tls/config-sample-mqtt-tls-client-auth.xml` | the vendor's own mTLS example — the one shipped file that takes the **mapping** branch of `client-authentication-mode`, so `REQUIRED → client_ca` is tested on vendor bytes rather than only on ours |
| `fixtures/hivemq-credentials-4.6.16.xml` | `hivemq/hivemq-file-rbac-extension` @ `4.6.16`, `README.adoc`'s example — plus two `<permission>` blocks marked `ADDED` inline | role flattening, and the `<qos>` / `<shared-subscription>` qualifier gaps (which the vendor's example does not contain, hence the two marked additions) |

**2. Composed fixtures.** Derived from vendor material and then deliberately changed, or
merged out of several vendor files. Necessary, because a broker's *stock defaults take the
negative branch of nearly every security mapping* — so a verbatim-only suite could prove
the refusals and never once prove that a positive mapping fires. Each header lists **every
value that differs from the vendor's, and why it differs**.

| Fixture | Composed how | What it proves |
|---|---|---|
| `fixtures/emqx-6.2.2.conf` | nine of those same example files (no `listeners.wss`), with **nine values changed** (`verify` → `verify_peer`, `fail_if_no_peer_cert` → `true`, `peer_cert_as_username` → `cn`, `max_retained_messages` → `100000`, two `max_connections` → finite and **unequal** (1000000 and 500000), `enable_ocsp_stapling` → `true`, two cipher strings shortened) and six blocks the vendor ships no example for at all (`authentication`, `authorization`, `rule_engine`, `bridges.mqtt`, `exhook`, `dashboard`) | `mtls_identity_source`, `max_retained_messages`, smallest-wins `max_connections` (500000 wins, with a TODO naming both), the authn/authz chain, the bridge translation — **and the mixed-posture refusal**: its `ssl` listener requires client certificates while its `quic` listener does not, so `client_ca` comes out **commented** with a TODO. The *positive* `client_ca` mapping needs a single-TLS-listener input, and is proven on `emqx-hostile-strings.conf` (and, on vendor bytes, by `hivemq-2026.5-tls-client-auth.xml`) |
| `fixtures/emqx-acl-documented-examples.conf` | six rules: **three copied** from the vendor `acl.conf`'s own documentation examples (cited by line), **three written here** and marked as such inline | rules that genuinely translate, plus the `{qos,retain}` qualifier gap and the first-match-wins → deny-wins ordering warning |
| `fixtures/hivemq-2026.5-config.xml` | a **merge** of five vendor files — four samples plus the default `config.xml` for `<anonymous-usage-statistics>` (a real `config.xml` holds one `<listeners>`, one `<mqtt>`, one `<restrictions>`, and the vendor ships those in separate samples) — plus `<security>` and `<persistence>` written from the shipped `config.xsd`'s element set and its documented defaults | the whole CE mapping surface in one document |

**3. Adversarial and hostile-input fixtures written for this repository**, not vendor
configuration — `emqx-adversarial.conf`, `emqx-hostile-strings.conf`,
`emqx-silent-drops.conf`, `hivemq-adversarial.xml`, `hivemq-multi-tls.xml`,
`hivemq-hostile-credentials.xml`. Their headers say so. They prove the properties the
contract rests on: unknown keys and unknown XML elements reported by path, whole
subsystems with no equivalent reported rather than dropped, malformed input handled
without a crash, TOML-hostile values (`CORP\jdoe`, Windows paths, quoted topics) surviving
into a config the broker actually loads, and the security-posture traps refusing to become
an mTLS mandate.

**Checked, not asserted.** Every provenance claim above was re-derived on 2026-08-14 by
re-fetching the vendor files and diffing, and each fixture header carries the command that
reproduces its own result:

- **The verbatim fixtures reproduce the vendor bytes exactly.** Each header gives a
  two-command check — one that strips the header off the fixture and hashes the rest, one
  that `curl`s the vendor path and hashes that — and both print the same SHA-256. For the
  concatenated EMQX file that is `sha256:5f1542e1…` over 21196 bytes (plus a per-part hash
  and byte count for all ten parts); for the two HiveMQ files, `9a8a93f3…` and `72f561d6…`.
- **The composed fixtures' deviation tables are complete.** For `emqx-6.2.2.conf` the
  vendor concatenation and the fixture were parsed with the converter's own HOCON reader
  and their leaf sets diffed: exactly the nine documented value changes, the one documented
  listener rename, and 53 dropped keys — no undocumented difference. For
  `hivemq-2026.5-config.xml` the same was done element by element against each cited vendor
  file: every "verbatim part" is equal path-for-path and value-for-value, with only comments
  dropped. For `emqx-acl-documented-examples.conf`, the three copied rules were located at
  the vendor `acl.conf`'s lines 97, 129 and 135, and the fragment behind one composed rule at
  line 123 — the header cites those line numbers so the split is checkable rule by rule.
- **The HiveMQ config fixtures validate against the vendor's own schema**
  (`src/main/resources/config.xsd` @ `2026.5`): `hivemq-2026.5-config.xml`,
  `hivemq-2026.5-default-config.xml`, `hivemq-2026.5-tls-client-auth.xml` and
  `hivemq-multi-tls.xml` all report *validates* under `xmllint`. That is how the composed
  file's `<security>` shape was caught and fixed: the XSD declares each toggle as a block
  containing `<enabled>`, so the flat form an earlier draft used could not occur in a real
  config. (`xmllint` is not in CI — the schema is not vendored — so this is a one-off check
  you can repeat with the two commands in the fixture headers.)
- **One tag needs a caveat, because the name is not what you would guess.** `6.2.2` in
  `emqx/emqx` is the tag whose GitHub release is titled *EMQX Enterprise 6.2.2* — since 6.x
  the vendor releases both editions from that repository, and these `rel/config/examples`
  files are not edition-specific. Concretely: `listeners.ssl.conf.example` is byte-identical
  at the open-source-titled tag `v5.8.9`, while `mqtt.conf.example` is not — which is why the
  5.x claim is bounded the way the next paragraph bounds it.

So: **no converter has ever been fed a config produced by a live EMQX or HiveMQ
broker.** The verbatim fixtures are vendor *source*, fetched from the vendor's repository
— not a config dumped out of a running instance, which is a different and stronger thing
this repository does not have.

**On the version ranges in the table above**, which are a parser claim and nothing more.
The fixtures are pinned at one tag for EMQX and one for HiveMQ — and **none at all for
Mosquitto**, whose mappings rest on `mosquitto.conf(5)` @ `v2.0.22` alone — so the other
versions rest on the parsers being tolerant rather than version-pinned. That much *was* spot-checked, on 2026-08-14 and
outside the CI suite: the ten `rel/config/examples` files at EMQX's open-source-titled tag
`v5.8.9`, concatenated, convert with **exit 0** and the result passes `mqttd --check-config`
(117 `TODO(migrate)` lines); so do HiveMQ CE's `src/main/resources/config.xml` and
`config-sample-mqtt.xml` at tags **2023.10** and **2024.9**. What that establishes is only
that the readers cope with those versions' shipped shapes — it is **not** a fidelity claim
about any mapping, and still not a live broker.

**And `--help` on each converter says so too**, in the same words — the paragraph above is
worth nothing if it lives only in a doc a hurried operator skips. All three epilogs carry the
same `VERSION SCOPE:` sentence (*"the version RANGE in docs/MIGRATION.md's What-ships table
is a PARSER claim and nothing more — one tag per broker is fixture-pinned where a fixture
exists at all, the neighbours rest on the reader being tolerant, and no mapping is
version-pinned. Nothing here was validated against a live broker."*) plus a `PROVENANCE:` line
naming that converter's own pinned tag.
Mosquitto's says what it has to say: **no vendor config file is pinned as a fixture for it at
all** — its mappings are written against `mosquitto.conf(5)` from
`eclipse-mosquitto/mosquitto` @ `v2.0.22`, and the Rust twin in `mqttui` prints the same
scope. Until 2026-08-15 `from-mosquitto.py --help` printed one line and disclosed none of
this, so the sentence above was itself unrun.

---

## Mosquitto → mqttd

```sh
scripts/migrate/from-mosquitto.py /etc/mosquitto/mosquitto.conf \
    --out-config mqttd.toml --out-acl acl.toml

mqttd --check-config --config mqttd.toml     # then read every TODO(migrate)
```

Or, with no checkout and no Python: `mqttui migrate mosquitto /etc/mosquitto/mosquitto.conf
--out-config mqttd.toml --out-acl acl.toml`, whose output is byte-identical (a differential
test holds it there).

**Two things about this converter that are not obvious**, both of them cases where
Mosquitto scopes something per listener and mqttd cannot:

- **`per_listener_settings true` makes EIGHT settings per-listener**: `password_file`,
  `acl_file`, `psk_file`, `allow_anonymous`, `allow_zero_length_clientid`,
  `auto_id_prefix`, `plugin` and `plugin_opt_*` — that is the list `mosquitto.conf(5)` @
  `v2.0.22` names, and it is the list the converter's own TODO, its `--help` and this row now
  all quote. (They previously said "exactly six" in four places against a document that names
  eight, and the two omitted are the pair that carries an entire third-party authn/authz
  backend.) mqttd's `[security]` is **node-wide**, so if two listeners disagreed only one
  value can survive: the converter takes the **last** one read — what Mosquitto itself does
  with `per_listener_settings` false — and emits a TODO naming every site and the value that
  won. If one listener really was anonymous and another was not, that difference is gone and
  the answer is two deployments.
- **`include_dir` is not followed, and neither is a `plugin`'s own config file.** Their
  contents are never read, and the TODO says so in those words rather than calling them
  unmapped settings: an included `.conf` can hold your whole
  `acl_file`/`password_file`/listener set, and `mosquitto.conf(5)` recommends the **Dynamic
  Security plugin over `password_file`** — for that deployment your entire user, role and ACL
  policy is in `dynamic-security.json`, which this converter never opens. When that is the
  shape it finds, the "no `acl_file`" TODO says so explicitly instead of concluding your old
  broker authorized everything (it derives that sentence from what it actually read).

### What maps

| Mosquitto | mqttd |
|---|---|
| `listener <port> [addr]` | `[listeners] plaintext_bind` / `tls_bind` / `ws_bind` / `wss_bind` — **one per protocol**, each carrying `# from: listener …`; the extras become TODOs naming their address. An address with no port anywhere in the input is **not** invented: the bind comes out commented with the decision named |
| `port` / `bind_address` (the **default listener**) | the same binds. Both directives were previously **unread**, so with TLS material present the bind was fabricated as `0.0.0.0:1883` — a `bind_address 127.0.0.1` broker published on every interface, on a port the input never named — and with no TLS material there was no `[listeners]` table at all and nothing said so. `bind_address` **without** `port` is deliberately still not a bind: `mosquitto.conf(5)` documents 1883 as the default, but the port is the one half of an address this tool will not supply for you (your real port may be in an `include_dir` file it never read), so the candidate is commented |
| `protocol websockets` | `[listeners] ws_bind` / `wss_bind` (with `certfile`). Previously unread, so a WebSocket listener was emitted as a **raw-MQTT** bind — every browser client breaks at cutover, and because a WSS listener counted as an ordinary TLS listener it also decided, silently, whose material won the single `[tls]` table. A `protocol` value that is neither `mqtt` nor `websockets` gets **no bind at all**, only a TODO: a transport cannot be guessed |
| `certfile` / `keyfile` | `[tls] cert` / `key`, from the **first** TLS listener — with a TODO naming every other listener's material, because the one `[tls]` table serves `tls_bind`, `wss_bind` and `quic_bind` alike |
| `cafile` **with** `require_certificate true` **on every TLS listener** | `[tls] client_ca` (mandatory mTLS). A **mixed** posture is not a mapping: the candidate comes out commented with a TODO naming which listeners required a certificate and which did not |
| `crlfile` | `[tls] crl` — **only when `client_ca` is active**. The broker refuses the pair otherwise (`invalid configuration: tls.crl requires tls.client_ca`), so with a commented `client_ca` the CRL is commented too and the TODO says to uncomment both together |
| `capath` | **nothing** — a directory of CAs is not supported, and the converter did not read it. TODO: concatenate into one PEM |
| `tls_version` | `[tls] allow_tls12` **when the value calls for it**. Mosquitto **2.x** reads this as a *minimum* — `mosquitto.conf(5)` @ `v2.0.22`, verbatim: "In Mosquitto version 1.6.x and earlier, this option set the only TLS protocol version that was allowed, rather than the minimum", so the minimum reading begins **after** 1.6.x, and an earlier draft of this row said "since 1.6", naming the last release where the claim was false. On 2.x: `tlsv1.3` matches mqttd's default and gets a NOTE, `tlsv1.2` gets the `allow_tls12` TODO (which also states the 1.6-and-earlier ONLY-version reading, because under it that listener was 1.2-only and *more* breaks), anything older gets "those clients cannot connect at all" |
| `use_identity_as_username true` | `[security] mtls_identity_source = "cn"` — an **exact** equivalent, and already mqttd's default; written out explicitly so the mapping is visible. `false` is a TODO, because mqttd always reads the identity from a presented certificate and your ACL keys change at cutover |
| `use_subject_as_username true` | **nothing** — `mtls_identity_source` offers `cn`, `san-dns`, `san-uri`, `san-email` only; there is no full-subject source |
| `allow_anonymous true` | **not carried over** — the candidate comes out **commented** in `[security]` with a TODO. mqttd refuses anonymous clients by default and `[security]` is node-wide, so activating it because one Mosquitto listener allowed it is a security-posture change applied to *every* listener, and a posture change is not a mapping (the #162 precedent, now applied without exception) |
| `acl_file` | the ACL policy to translate, **and** `[security] acl_file` in the generated config. Without that key mqttd enforces no authorization at all, so a config with no `acl_file` gets a TODO saying exactly that |
| `max_connections` | `[limits] max_connections`, **smallest-wins** across listeners with a TODO naming every site: `mosquitto.conf(5)` scopes it to "the current listener" and mqttd's is node-wide, so a flat mapping collapsed several listeners last-wins and a deliberately tight cap on a device listener was silently replaced by a browser listener's large one. `-1` (the vendor's documented spelling of unlimited, and what the shipped `mosquitto.conf` carries) leaves the key **unset** with a NOTE — passing it through produced `max_connections = -1`, which `mqttd --check-config` **rejects** (`invalid value: integer -1, expected u64`) |
| `max_queued_messages` / `max_packet_size` | the matching `[limits]` keys |
| `message_size_limit` | `[limits] max_packet_size` + a NOTE — the **nearest** equivalent, **not the same quantity**. `mosquitto.conf(5)` @ `v2.0.22` defines `message_size_limit` as "the maximum publish **payload** size that the broker will allow", while its own `max_packet_size` "applies to the full MQTT packet, **not just the payload**" — and mqttd's key is the packet form too. So the migrated cap is *tighter* than yours by each publish's fixed header, topic and MQTT 5 properties. An earlier version of this row (and of the NOTE the converter wrote into the deployed config) said the man page **deprecates** `message_size_limit` in favour of `max_packet_size` and that the two are the same quantity: the page says neither — it marks `port`, `bind_address`, `allow_duplicate_messages` and `clientid_prefixes` deprecated, and not this one. `0` is the vendor's documented spelling of *no limit* ("The default value is 0, which means that all valid MQTT messages are accepted"), so it leaves the key **unset** with a NOTE — passing it through wrote `max_packet_size = 0`, which `--check-config` accepts and the broker **floors to 1024**, refusing every packet over 1 KiB |
| `max_topic_alias` | `[limits] topic_alias_max` (clamped to 65535 with a TODO if larger) — an exact equivalent that was previously reported as having none |
| `persistence_location` | `[node] data_dir` (defaulted to `/var/lib/mqttd` with a NOTE when absent, because the broker refuses to start without one) |
| `max_inflight_messages` | **deliberately not mapped.** It bounds messages the broker may have in flight *toward* a client; `[limits] receive_maximum` is the *inbound* window mqttd grants. Opposite directions, so it is a TODO with a commented candidate |
| ACL `user X` + `topic …` / `pattern …` | `[[rules]]` with explicit `identities`; `%u` → `%i`, `%c` → `%c`. A `pattern` line applies to all users, so it becomes a rule with **no** `identities` |
| ACL `topic …` lines **before the first `user` line** | `[[rules]]` scoped to `identities = ["anonymous"]` — the subject mqttd gives an unauthenticated client (`crates/mqtt-auth/src/basic.rs`) — plus a TODO quoting `mosquitto.conf(5)`: "The first set of topics are applied to anonymous clients, assuming allow_anonymous is true". They used to be emitted with **no** `identities`, which mqttd applies to **every authenticated client**: strictly broader than the source under both postures (under `allow_anonymous false` those topics were reachable by nobody). They grant nothing until `allow_anonymous` is uncommented |
| A username containing a literal `*`, or a plain `topic` filter containing `%c`/`%i` | **no rule**, plus a TODO naming the value. mqttd's `identities` are globs and its topic patterns substitute `%c`/`%i`, in both cases with **no escape** (`crates/mqtt-auth/src/acl.rs`), so either construct could only be emitted *wider* than the source. Refused instead — see [KNOWN GAPS](#known-gaps-after-round-4) |
| `psk_file` / `psk_hint` (a TLS-**PSK** listener) | **no bind at all.** The listener was ENCRYPTED (`mosquitto.conf(5)`: "The psk_hint option enables pre-shared-key support for this listener") and mqttd has no PSK ciphersuites, so the candidate is emitted **commented out on the TLS key** of its transport with a TODO: converting it to `plaintext_bind` — which is what happened until 2026-08-15, with a genuine `# from:` on the line — downgrades an encrypted listener to cleartext. `psk_file`'s identities each need a new credential |
| A `connection` block and its `address` / `topic` / `bridge_cafile` / `bridge_certfile` / `bridge_keyfile` / `remote_username` / `remote_password` / `remote_clientid` | **reported per key, each naming its `mqtt-bridge` equivalent** (`[[upstreams]] url`, `[[upstreams.rules]]`, `[upstreams.tls] ca`/`cert`/`key`, `username`, `password_file`, `client_id`). No bridge config is written — this converter has no `--out-bridge`. All but `connection` used to be reported as "no direct equivalent — check the mqttd configuration table", which has nothing to find, `bridge_cafile` included |
| An address the broker cannot bind (`listener 0 /tmp/mosq.sock`, a non-numeric port) | **no live bind**: the candidate is commented with the reason. `mqttd --check-config` accepts any string in a bind and the broker then fails at startup, so the verification this page points you at did not cover it (invariant **H** now does). A UNIX-socket listener declares no TCP endpoint at all — mqttd has no unix-socket transport |

---

## EMQX → mqttd

```sh
scripts/migrate/from-emqx.py /etc/emqx/emqx.conf \
    --acl-file /etc/emqx/acl.conf \
    --out-config mqttd.toml --out-acl acl.toml --out-bridge bridge.toml

mqttd --check-config --config mqttd.toml     # then read every TODO(migrate)
```

**Read this before you trust an empty result.** EMQX persists dashboard- and
REST-managed configuration to `data/configs/cluster.hocon`, *not* `emqx.conf`. If
your authn chain and your ACL rules were set up through the dashboard, they are in
that file — pass it instead (the converter accepts either). And EMQX's
`built_in_database` user table and ACL table live in the **data directory**, not in
any config file at all: the converter cannot see them, and says so rather than
implying there was nothing there.

### What maps

| EMQX | mqttd |
|---|---|
| `node.name` (local part) | `[node] id` |
| `node.data_dir` | `[node] data_dir` |
| `listeners.tcp.*` / `.ssl.*` / `.ws.*` / `.wss.*` / `.quic.*` `bind` | `[listeners] plaintext_bind` / `tls_bind` / `ws_bind` / `wss_bind` / `quic_bind` — **one per protocol** |
| `ssl_options.certfile` / `.keyfile` | `[tls] cert` / `key` |
| `ssl_options.cacertfile` **with** `verify = verify_peer` **and** `fail_if_no_peer_cert = true` | `[tls] client_ca` (mandatory mTLS) |
| `mqtt.max_packet_size` (`1MB` → bytes) | `[limits] max_packet_size` |
| `mqtt.max_topic_alias` | `[limits] topic_alias_max` |
| `mqtt.max_mqueue_len` | `[limits] max_queued_messages` |
| `mqtt.max_subscriptions` | `[limits] max_subscriptions_per_client` |
| `mqtt.peer_cert_as_username = cn` | `[security] mtls_identity_source = "cn"` |
| listener `max_connections` | `[limits] max_connections` — mqttd's cap is **node-wide**, so several listeners collapse onto the **smallest** of them, and a TODO names the values it collapsed. `infinity` (the vendor's default on every shipped listener) sets nothing, which is also uncapped, and says so in a NOTE |
| listener `messages_rate = "N/s"` | `[limits] max_publish_rate` |
| `retainer.backend.max_retained_messages` | `[limits] max_retained_messages` |
| `authentication [password_based/http]` `url` | `[security.http_auth] url` — **with a changed contract, see below**. Every other key on that authenticator (`method`, `headers`, `body`, `pool_size`, the whole `ssl.*` block) is a per-key TODO with the reason its loss breaks login |
| `authentication` on any **other** backend (`mysql`, `redis`, `postgres`, `mongodb`, `ldap`, `scram`, or a mechanism this converter does not know) | **nothing** — and every key it carried is now named, the `server`/`database`/`query`/`cmd` included, because the credential store it read is the one fact you need to rebuild the check behind `[security.http_auth]`. Until 2026-08-15 those keys appeared NOWHERE, under a reassuring per-mechanism TODO — on the backend this repository's own pinned fixture exercises |
| `authentication [jwt]` `verify_claims = { iss = …, aud = … }` | `[security.jwt] issuer` / `audience` — **`iss` and `aud` only**. EMQX has no `issuer` or `audience` field at all (`apps/emqx_auth_jwt/src/emqx_authn_jwt_schema.erl` @ `6.2.2`: `mechanism`, `acl_claim_name`, `on_missing_jwt`, `verify_claims`, `disconnect_after_expire`, `from`; `grep -cE 'issuer\|audience'` returns 0), so an earlier version of this row described input EMQX cannot produce while the real construct was dropped. Any *other* claim in `verify_claims` is a TODO: mqttd verifies the signature, the expiry and those two claims and nothing else, so an unmapped constraint means a correctly-signed token now passes |
| `authorization.no_match` | the ACL document's `default` — and every sentence the converter writes about what that policy *does* is derived from this value, not from a constant. `no_match = allow` produces `default = "allow"`, so a policy that translated nothing is **wide open**, and the TODO in it says exactly that instead of claiming it fails closed |
| listener `enable` / `enabled` = `false` | **nothing** — the listener is NOT bound, and a TODO names it, its address and everything it carried. It is a real `base_listener` field (`apps/emqx/src/emqx_schema.erl` `base_listener/1` @ `6.2.2`, default `true`, alias `enabled`), and carrying a switched-off listener over is the one flip that opens a network port |
| listener `enable_authn = false` | **nothing** — authentication is node-wide in mqttd. A TODO says that listener accepted clients without authenticating them and that they now need credentials or a separate deployment; `enable_authn = true` gets a one-line "matches mqttd's posture" TODO instead of the same sentence |
| `authorization.sources [file] path` | the ACL file to translate |
| `bridges.mqtt.*` ingress/egress (the **v1** shape) | `mqtt-bridge` `in` / `out` rules (`--out-bridge`) |
| a bridge/connector `ssl { enable = true, … }` | `[upstreams.tls]` is emitted **commented out**, naming every path the EMQX side held (they are paths on the EMQX host, and `mqtt-bridge` runs elsewhere) — **and so is that upstream's `url`**. `mqtt-bridge`'s `tls` block is optional and **absent means plaintext**, so a live `url` beside a commented `tls` block was a live posture downgrade: completing the draft exactly as the file instructs sent the bridge's CONNECT, username included, in the clear to a peer that expected TLS. Both lines are now inert, and `mqtt-bridge` refuses to start without a `url` |
| `connectors.mqtt.*` + `actions.mqtt.*` / `sources.mqtt.*` (the **v2** shape, which is what `6.2.2` actually ships) | the connector becomes the `[[upstreams]]` address and credentials; each action's `local_topic` + `parameters.topic` becomes an `out` rule, each source's `parameters.topic` + `local_topic` an `in` rule. `bridges` is **not a root in 6.2.2's schema at all** (`emqx_conf_schema:roots/0`, `emqx_bridge_v2_schema:roots/0`) and survives only through the vendor's v1 upgrade path — so a row naming only `bridges.*` described a shape a current EMQX does not write. `parameters.retain` and `parameters.payload` are still per-key TODOs: `mqtt-bridge` forwards the payload byte for byte and preserves the source retain bit |

### What deliberately does not, and why

Every one of these is a `TODO(migrate)` line naming what you must decide:

- **The SQL rule engine, and data integration** (`connectors` / `actions` / `sources`,
  ex-`bridges.*`). mqttd is a broker, not an integration platform. Only *MQTT-type*
  connectors have an analogue (`mqtt-bridge`); every Kafka / HTTP / JDBC / S3 sink must
  become a client-side consumer you own — and that consumer has a designed, CI-tested
  shape: the external-consumer blueprint in [INTEGRATION.md](INTEGRATION.md) (ADR 0063),
  including the rule-construct-by-construct mapping table. **A rule you forget is a
  data pipeline that silently stops.**
- **Gateways** (CoAP, LwM2M, MQTT-SN, STOMP, ExProto, GBT32960, OCPP). mqttd speaks
  MQTT 3.1.1/5 over TCP, TLS, WS, WSS and QUIC only.
- **`exhook` and `plugins`.** There is no hook API and no plugin ABI. An
  *auth-shaped* hook maps to `[security.http_auth]`; a message-mutating hook does not
  map at all.
- **The dashboard and the REST API.** Absent by design (ADR 0020): `/metrics`,
  `/statusz`, an audit log, and config + SIGHUP. *Plan the operator workflow that
  replaces the dashboard before cutover, not after.*
- **`zones`.** mqttd's config is node-wide. Every zone-scoped setting must collapse to
  one value, or the zones become separate deployments.
- **Non-Argon2id password backends.** `built_in_database` hashes are sha256/bcrypt/
  pbkdf2; they cannot be converted. `mysql` / `postgresql` / `mongodb` / `redis` /
  `ldap` have no native mqttd backend — the supported path is one
  `[security.http_auth]` hook that *you* write against the store you already run. It
  is operator code, not a shipped feature.
- **`cluster.discovery_strategy`.** Not translated, deliberately: mqttd's mesh needs a
  per-node bus certificate whose CN equals `[node] id`, a shared 64-hex gossip key,
  and the founder rule — none of which an EMQX discovery strategy expresses. Walk
  [the secured cluster tutorial](SECURED-CLUSTER-TUTORIAL.md).
- **OCSP stapling.** Revocation is a CRL file (`[tls] crl`).
- **`$SYS`.** Not implemented. Any client that subscribed to it must be rewritten
  against `/metrics`.
- **`mqtt.max_inflight` — deliberately not mapped, because the nearest-looking setting
  runs the other way.** EMQX's `max_inflight` bounds messages the **broker sends to a
  client**. mqttd's `[limits] receive_maximum` is the inbound window it **grants clients**
  (`crates/mqtt-config/src/lib.rs`: "MQTT 5 Receive Maximum granted to clients"); the
  broker→client direction is not configurable at all, because mqttd honours each v5
  client's own Receive Maximum (`conn.rs`). Mapping the two would have been quietly
  destructive: EMQX ships `max_inflight = 32` and mqttd defaults `receive_maximum = 256`,
  so every stock conversion would have cut the inbound window **8×**. The converter emits
  a TODO stating the flip and offers a commented `receive_maximum` line.
- **Per-TLS-listener settings, when you have more than one TLS listener.** mqttd has **one
  `[tls]` table**, and it applies to `tls_bind`, `wss_bind` *and* `quic_bind` at once. So
  the first TLS listener's material and posture become every TLS transport's, and the
  others' `ssl_options` — `verify`, `versions`, `enable_crl_check`, `depth`, ciphers,
  their PEM paths — are referenced nowhere. Every one of them comes out as a TODO naming
  the listener it came from, and if the listeners **disagree** about client certificates
  the `client_ca` line is emitted **commented out**: mapping it would newly demand certs
  from the clients of the listener that did not require them, and dropping it would lose a
  live mandate. Neither is a mapping.

**One behavioural difference worth stopping on:** EMQX's HTTP authentication reads a
JSON body (`{"result":"allow"}`); mqttd reads the **HTTP status code** — 200 allow,
401/403 deny, anything else (timeout, unreachable host) **deny**. The converter
carries the URL over and says loudly that your endpoint almost certainly needs a
change. Verify the status codes it returns today.

### The ACL, and the ways a conversion can be *more permissive*

Two of them, and the second was found in round 4. **(1) Ordering**, below. **(2) SCOPE**: a rule
whose source scoped it to *some* clients, emitted unscoped or with a wider matcher, applies to
more of them. Everything known in that class is in [KNOWN GAPS](#known-gaps-after-round-4) —
Mosquitto's anonymous-scoped `topic` block (which used to become a grant to every authenticated
client), a literal `*` in a username becoming a glob, a literal `%c` in a `topic` filter
becoming a live per-client grant, and HiveMQ's `<qos>`/`<retain>`/`<shared-subscription>`
qualifiers, which mqttd cannot express and which are emitted *covering everything* with a TODO.
The first three are now refused or scoped; the fourth is still the broader direction and says so.

EMQX walks `acl.conf` in **file order and stops at the first match**. mqttd is
**deny-wins**: every rule is considered, any matching deny beats every allow, then any
matching allow permits, then `default`. A policy that relied on an early `allow`
shadowing a later `deny` **changes meaning**. When both effects are present the
converter emits that warning first, in capitals, at the top of the ACL file.

These EMQX conditions cannot be expressed at all, and the affected rule is **not
emitted** (a rule that would change your posture is not a mapping):

| EMQX construct | Why not, and what to do |
|---|---|
| `{ipaddr, …}` / `{ipaddrs, …}` | no address matcher — authorization is identity + topic. Address policy belongs in the network layer |
| `{username, {re, …}}` / `{clientid, {re, …}}` | `identities` are **globs**, not regexes — and `*` is the **only** special character (`crates/mqtt-auth/src/acl.rs` `glob_match`). Every other byte, `?` included, matches literally, so a regex `.` translated to `?` matches nothing |
| `{clientid, "x"}` on a publish/subscribe rule | client-id matching exists only for `connect` rules, which carry no topics. Put `%c` in the *topic* instead |
| `{client_attr, …}` / `{zone, …}` / `{listener, …}` | no client attributes, no zones, no per-listener conditions |
| `{'and', […]}` / `{'or', […]}` | rules match any-of `identities` and any-of `groups`; no boolean combinator |
| `{eq, "#"}` | no exact-filter matcher. Allow rules match by **coverage**, deny rules by **overlap** — so a deny on `#` would deny everything |
| `{security_profile, legacy}` | `EMQX_SECURITY_PROFILE` has no analogue |
| `{publish, [{qos,0},{retain,false}]}` | no qos/retain qualifier. The rule **is** emitted, without the qualifier — which makes an `allow` **broader**. The TODO says so |

The vendor's shipped default `acl.conf` is a good demonstration: all four of its rules
land on a different one of these gaps. Measured on the verbatim fixture (and pinned by an
assertion in `test-from-emqx.sh`, so this page cannot drift away from the tool): **six**
`TODO(migrate)` lines and **one** emitted rule. Six rather than four because the third
vendor rule contributes three on its own — one for its `$SYS` topic and one for each of
its two `{eq, …}` entries. The one emitted rule is that same rule's `$SYS/#` deny, kept
for the record and reported as **inert**, since mqttd implements no `$SYS` tree.

Placeholders that do translate: `${username}` → `%i`, `${clientid}` → `%c`,
`${cert_common_name}` → `%i` (**only** equal when the client used mTLS and
`mtls_identity_source = "cn"`, which the TODO tells you to check). mqttd's `%i`/`%c`
**fail closed** on an empty value or one containing `/ + #`. A topic carrying a
**literal** `%c` or `%i` is refused outright — EMQX 5/6 matches those bytes literally
(the pinned `acl.conf` schema lists its placeholders and `%c`/`%i` are not among them),
so emitting them into a rule mqttd *substitutes* would grant a per-client namespace the
source never granted. See [KNOWN GAPS](#known-gaps-after-round-4).

---

## HiveMQ → mqttd

```sh
scripts/migrate/from-hivemq.py /opt/hivemq/conf/config.xml \
    --credentials /opt/hivemq/extensions/hivemq-file-rbac-extension/credentials.xml \
    --out-config mqttd.toml --out-acl acl.toml

mqttd --check-config --config mqttd.toml     # then read every TODO(migrate)
```

**HiveMQ Community Edition has no authentication and no authorization at all** —
both come from extensions. A CE `config.xml` on its own therefore describes an
*anonymous* broker. mqttd refuses anonymous clients by default, so plan the credential
rollout as part of the cutover; the converter says this rather than leaving you to
discover it when nothing connects.

Version note: **HiveMQ CE is calendar-versioned** (2023.x … 2026.5). "HiveMQ 4.x" now
refers to the Enterprise line and the extension SDK.

### What maps

| HiveMQ | mqttd |
|---|---|
| `tcp-listener` / `tls-tcp-listener` / `websocket-listener` / `tls-websocket-listener` | `plaintext_bind` / `tls_bind` / `ws_bind` / `wss_bind` — **one per protocol** |
| `mqtt/packets/max-packet-size` | `[limits] max_packet_size` |
| `mqtt/receive-maximum/server-receive-maximum` | `[limits] receive_maximum` — **same direction**: both are the inbound window the *server* grants a client. (Contrast EMQX's `max_inflight`, which runs the other way and is therefore [not mapped](#what-deliberately-does-not-and-why).) HiveMQ's default is 10, mqttd's 256, so carrying the value over *tightens* the window — which is what fidelity means here |
| `mqtt/topic-alias/max-per-client` | `[limits] topic_alias_max` |
| `mqtt/queued-messages/max-queue-size` | `[limits] max_queued_messages` |
| `mqtt/queued-messages/strategy` = `discard` | `[limits] queue_overflow = "reject-newest"` |
| `mqtt/queued-messages/strategy` = `discard-oldest` | `[limits] queue_overflow = "drop-oldest"` |
| `restrictions/max-connections` (`-1` = unlimited → unset) | `[limits] max_connections` |
| `tls/client-authentication-mode` = `REQUIRED` | `[tls] client_ca` (mandatory mTLS) — **only when every TLS listener says REQUIRED**; a mixed posture is not a mapping, [see below](#the-two-findings-that-shape-the-output) |
| File RBAC `<user>` + `<role>` + `<permission>` | ACL `[[rules]]`, flattened onto `identities` |
| `${{clientid}}` / `${{username}}` | `%c` / `%i` — those two are file-RBAC's **only** substitutions, so a `<topic>` carrying a **literal** `%c`/`%i` matched those bytes exactly and is refused rather than emitted into a rule mqttd would substitute ([KNOWN GAPS](#known-gaps-after-round-4)) |

`<path>/mqtt</path>` needs no translation: **mqttd accepts a WebSocket upgrade on any
path** (it checks the `mqtt` subprotocol, not the URI — `crates/mqtt-net/src/ws.rs`),
so existing browser clients keep working. But a listener that offered `mqttv3.1` as a
subprotocol gets a TODO, because mqttd negotiates **only** `mqtt` and refuses an
upgrade that does not offer it.

### The two findings that shape the output

**1. TLS material is a Java keystore, and mqttd wants PEM paths.** There is no
conversion. The converter emits an extraction recipe and never touches the key
material:

```sh
keytool -importkeystore -srckeystore keystore.jks \
    -srcstoretype JKS -destkeystore server.p12 -deststoretype PKCS12
openssl pkcs12 -in server.p12 -nokeys  -out server.crt -legacy
openssl pkcs12 -in server.p12 -nocerts -nodes -out server.key -legacy
keytool -list -rfc -keystore truststore.jks > client-ca.crt
openssl pkey -in server.key -out server.key.pem
```

**How far that is verified:** the `openssl` steps were **run**, against a PKCS#12
minted locally, and mqttd booted a real TLS listener on the extracted PEM pair
(bag-attribute preamble included — rustls skips it). The two `keytool` steps were
**not run**: the machine this was authored on has no Java runtime and no
HiveMQ-generated keystore was available. Check them against your JDK.

`client-authentication-mode` maps onto mqttd's posture gate exactly **when every TLS
listener agrees**: unanimous `REQUIRED` → `client_ca`; `OPTIONAL` → `client_ca`
**commented out** plus a TODO, because mqttd has no cert-optional mode and silently
mandating mTLS would lock out every client without a certificate; `NONE` → no
`client_ca`.

**When they disagree, there is no mapping, and the converter does not pick.** mqttd has
one `[tls]` table serving `tls_bind`, `wss_bind` and `quic_bind`, so a HiveMQ deployment
with `REQUIRED` on `tls-tcp-listener` and `NONE` on `tls-websocket-listener` cannot be
expressed: mapping `client_ca` would newly demand certificates from every browser client,
and omitting it would drop a mandate that is live today. The candidate line comes out
commented, with a TODO naming which listeners required certificates and which did not. The
same holds for `<protocols>` and for the truststore — the extraction recipe emits a
`keytool` line for **every** truststore, not just the first listener's, and tells you to
concatenate them.

**2. Roles cannot become mqttd `groups`.** mqttd populates `groups` only from an OIDC
token's `groups_claim` or the HTTP auth hook's `{"groups":[…]}` body — the Argon2id
password file always yields an empty group list
(`crates/mqtt-auth/src/password.rs`). So file-RBAC roles are **flattened** into
per-user `identities` rules: correct, but the file grows with users × permissions, and
a role change means re-running the converter. The alternative (move authentication to
OIDC or the HTTP hook, then hand-write `groups = [...]` rules) is named in a TODO.

### What deliberately does not map

- **Every HiveMQ Enterprise construct** — `<cluster>`, `<control-center>`,
  `<license>`, the Enterprise Security Extension, the Enterprise Bridge Extension —
  and **the whole extension SDK**. Enterprise's schema is not open source; the
  converter recognises these by name and reports them, which is the honest handling.
  It has never read their schema and does not pretend to.
- **Permission qualifiers.** `<qos>`, `<retain>`, `<shared-subscription>` and
  `<shared-group>` have no mqttd equivalent. The rule **is** emitted, without the
  qualifier — **broader than the original** — and each one gets a TODO saying so.
- **`max-qos`, `max-client-id-length`, `max-topic-length`,
  `no-connect-idle-timeout`, `incoming-bandwidth-throttling`**, the
  `wildcard-subscriptions` / `shared-subscriptions` / `retained-messages` on-off
  switches, the session- and message-expiry caps, and the `<security>` validation
  toggles. Each is either always-on, always-off, or not configurable in mqttd, and the
  TODO says which.
- **`anonymous-usage-statistics`.** mqttd sends no telemetry anywhere, so there is
  nothing to disable.

Note one agreement worth keeping: the file-RBAC extension **prohibits** `#` and `+` in
usernames and client ids and denies such connections; mqttd's `%i`/`%c` substitutions
**fail closed** on exactly those characters. The two brokers agree, so a name like
that was already broken — fix it, do not carry it over.

---

## The dual-run cutover

**mqttd cannot import another broker's session state.** A persistent session's offline
queue, its subscriptions, its in-flight QoS 2 exchanges and its will message do not
migrate; a moved client must resubscribe. That is why cutover is a dual run rather
than a switch.

**Retained state is the exception, and it migrates by itself.** The bridge subscribes
with `retain_as_published = true` and `retain_handling = 0`, and the engine preserves
the source retain bit, so on each connect it receives the far side's retained *set* and
republishes it with RETAIN intact — an idempotent re-sync, not a fake-live storm.
Verified below.

> **The hazard on the other side of that convenience: a deleted retained value can come
> back.** The re-sync runs in **both directions on every reconnect**, so under a `both`
> rule the surviving copy wins and a tombstone is not idempotent. Measured: a retained
> `fleet/truck7/config` had crossed to mqttd; the bridge was stopped; the value was cleared
> on the incumbent with `mosquitto_pub -r -n` and confirmed gone; the bridge was restarted
> — and the value was retained on the incumbent again, republished from mqttd. Any
> reconnect is enough: a restart, a network blip, a rolling deploy. There is no log line
> attributing it, and step 4 explicitly tells clients they can rely on retained values
> already being there, so this sits in the path this playbook recommends.
>
> **Mitigation, and it is tested (`dual-run-smoke.sh` assertion 6): prune retained state
> with the bridge RUNNING, never while it is stopped.** A retained clear is a zero-length
> retained publish, and it crosses the bridge like any other message — so the tombstone
> propagates and the value stays gone on both sides. Assertion 6 is exactly that much:
> prune on the incumbent with the bridge up, then find nothing on the incumbent *and*
> nothing on mqttd. The follow-on question — does it stay gone once the bridge reconnects,
> which is what actually bit in the hazard above? — was measured by hand on the same shapes
> rather than by the harness: after a bridge restart the value was still absent on both
> brokers. Then verify on **both** brokers before you call the prune done — subscribing on
> the side you deleted from is not enough to know the other side agrees.

### Step 0 — before anything: audit the certificates

mqttd **requires the `clientAuth` extended key usage** on a client certificate and
refuses one without it at the TLS handshake. OpenSSL-based brokers tolerated EKU-less
device certs for years, so a migrating fleet discovers this by outage. It is also TLS
1.3-only by default. Both are checkable in advance:

```sh
scripts/migrate/cert-audit.sh /path/to/device/certs      # exit 1 if any cert is a blocker
```

### Step 1 — convert, validate, and read every TODO

```sh
scripts/migrate/from-emqx.py emqx.conf --acl-file acl.conf \
    --out-config mqttd.toml --out-acl acl.toml
mqttd --check-config --config mqttd.toml
grep -c 'TODO(migrate)' mqttd.toml acl.toml
```

`--check-config` passing is necessary and not sufficient. **The TODOs are the work.**

### Step 2 — stand mqttd up empty, beside the incumbent

Nothing points at it yet. Use the
[secured three-node tutorial](SECURED-CLUSTER-TUTORIAL.md) or the README's secured
single-node container. Verify: `/readyz` returns 200, and the startup log contains no
`INSECURE` line.

That second check is worth doing because it is not vacuous: every opt-out of a secure
default is logged at **WARN**, so it survives the default log level and any `RUST_LOG` that
admits warnings. Measured on this tree — a plaintext, anonymous node logs **three**
`INSECURE` lines (plaintext listener, no ACL file, anonymous permitted); a secured one logs
none. Absence is therefore a signal, not silence. (The **audit** log is different: it is
`tracing` target `audit` at **INFO**, so a `RUST_LOG=warn` deployment gets the security
warnings and none of the ACL-denial events — see step 5.)

### Step 3 — bridge them

`mqtt-bridge` is a **separate process** ([ADR 0025](adr/0025-boundary-bridge.md),
[docs/BRIDGE.md](BRIDGE.md)), an ordinary MQTT client to both sides. Forwarding is
**deny by default**: only a topic matching a rule crosses, and only in that rule's
direction. For a cutover the correct shape is a **`both` rule with no remap** over the
namespace both brokers share:

```toml
hop_count_limit = 8
share_group = ""                        # one instance; sharing off

[local]
url = "mqttd-1:1883"
client_id = "cutover-bridge-local"      # MUST be unique per instance
username = "bridge"                     # a least-privilege account per side
password_file = "/run/secrets/bridge-local"

[spool]
dir = "/var/lib/mqtt-bridge"            # a QoS>=1 rule is REFUSED without this
max_messages = 10000

[[upstreams]]
name = "incumbent"
url = "legacy-broker:8883"
client_id = "cutover-bridge-incumbent"
username = "mqttd-bridge"
password_file = "/run/secrets/bridge-upstream"
tls = { ca = "/etc/bridge/ca.crt", cert = "/etc/bridge/client.crt", key = "/etc/bridge/client.key" }

[[upstreams.rules]]
direction = "both"
filter = "fleet/#"
qos = 1                                 # the DEFAULT IS 0 — omitting this downgrades
```

Five things in there are load-bearing, and four of them are refusals the binary
enforces:

- **`remap` is rejected on a `both` rule** (issue #192): the same strip/prefix applied
  both ways double-prefixes the reverse leg. Split it into an explicit `out` rule and
  an `in` rule if you need one.
- **A filter starting with `$` is rejected** (issue #193). `$SYS/#` and `$share/…` are
  never bridged, so a legacy `$SYS` tree cannot be mirrored.
- **A QoS ≥ 1 rule is refused without `[spool] dir`** (ADR 0060 T4): the source's ack
  is meant to be gated on durability, and an in-memory spool loses acked messages on
  restart. **That volume holds production payloads in the clear — encrypt it at rest.**
  (The refusal names one escape hatch, `[spool] allow_ephemeral_spool = true`, whose whole
  meaning is "I accept message loss on restart". Do not take it during a cutover.)
- **`qos` defaults to 0.** Forgetting it silently downgrades every migrated message.
  QoS 2 is downgraded to 1 by the engine in any case (ADR 0025 §7).
- **An mTLS half-identity is refused** — `cert` and `key` together, or neither.

Loop prevention has two levels and the primary one is the strong one: every bridge
subscription sets **`no_local = true`**, so the broker never echoes back a message the
bridge itself published — a property of the *subscription*, which no publisher can
forge. The `fss-bridge-hop-count` user property (default limit 8) is the backstop for a
multi-broker cycle; it is attacker-settable (issue #191) and it needs every broker on
the path to speak MQTT 5.

### Step 4 — move clients in cohorts

Cheapest cohort first. **A clean-start cohort is the safe first wave**, because it has
no session state to lose. For a persistent-session cohort, state plainly to whoever
owns those clients: the queue, the subscriptions and any in-flight QoS 2 exchange are
gone, and the client must resubscribe on its first connect to mqttd. Retained values it
depends on are already there (step 3).

### Step 5 — verify, on every cohort

| Check | Command |
|---|---|
| cross-broker round trip, both ways | `mosquitto_pub` on one, `mosquitto_sub` on the other |
| no amplification, **each way separately** | publish once, count deliveries — must be exactly 1. Do it in *both* directions: the inbound count tests mqttd's No Local, the outbound count tests the incumbent's (see [what was run](#what-was-actually-run-and-what-was-not)) |
| retained values arrived | subscribe fresh on mqttd, expect the retained payload |
| ACL denials are landing | the **audit log**, which is `tracing` target `audit` at INFO on the broker's own log — so `RUST_LOG` must admit it. Event kinds: `acl.deny.publish`, `acl.deny.subscribe`, `acl.deny.connect`, `acl.deny.will`. **One** of those four also increments a Prometheus counter, and only one: a CONNECT refused for an unauthorized **will topic** raises `mqttd_connection_errors_total{reason="acl"}` (`conn.rs`, `count_connection_error(policy, "acl")`). Publish, subscribe and connect-ACL denials are audit-log-only. A migrating fleet's will topics are a classic ACL miss, so alert on that counter *and* scrape the log — neither alone covers the four kinds |
| the node is serving | `mqttd --probe /readyz` — **exit 0** ready, **1** not ready or unreachable, **2** when it cannot tell where to look. It reads `MQTTD_HEALTH_BIND` from the environment or the `--config` file, so give it one of those or `--url <host:port>`; measured: 0 against a live broker, 1 against a dead one, 2 with no health endpoint configured |
| the bridge is healthy | its `metrics_bind` `/metrics`. **The series carry the `fss` registry prefix** — an alert on the unprefixed name returns no data forever: `fss_bridge_connected{side}`, `fss_bridge_forwarded_total{upstream,direction}`, `fss_bridge_dropped_total{reason="hop-limit"}` and `fss_bridge_dropped_total{reason="spool-full",side}` (the second is **real message loss**), `fss_bridge_spool_depth{side}` against `fss_bridge_spool_capacity{side}`, `fss_bridge_reconnects_total{side}`. Every one of those is asserted **anchored** by `dual-run-smoke.sh` against a running bridge, and the same assertion fails if an unprefixed `bridge_*` series ever appears |

### Step 6 — narrow, then cut

Change the `both` rule to one-way (`in` only) once no client on mqttd needs to reach
one on the incumbent. The engine then **never subscribes on the closed side** — it is
enforced in code, not just config — so the direction change is auditable rather than
aspirational. Then remove the rule, stop the bridge, and retire the incumbent.

### Rollback

Re-widen the rule to `both`. The incumbent is still live and still holds its own
sessions, which is the entire reason for running two brokers instead of one switch.

### What was actually run, and what was not

`scripts/migrate/dual-run-smoke.sh` runs **step 3's bridge config, verbatim in shape**,
against real binaries. Executed 2026-08-14 on macOS 14 (Darwin 23.5.0) with **mqttd 0.9.0**
(tree build) and **mosquitto 2.1.2** as the incumbent stand-in:

```
versions under test:
  incumbent stand-in: mosquitto version 2.1.2
  mqttd:              mqttd 0.9.0
  ok   — the incumbent broker is up on 127.0.0.1:61873
  ok   — a RETAINED message pre-exists on the incumbent (fleet/truck7/config)
  ok   — mqttd is up and READY on 127.0.0.1:61874 (empty: no sessions, no retained set)
  ok   — mqtt-bridge accepted the playbook's config and connected to both brokers
  ok   — 1. incumbent -> mqttd: a QoS 1 publish crosses inbound
  ok   — 2. mqttd -> incumbent: a QoS 1 publish crosses outbound (a moved client can
             still reach an unmoved one)
  ok   — 3. RETAINED state crossed on its own and is retained on mqttd (a fresh
             subscriber gets it)
  ok   — 4. inbound: one publish, exactly one delivery — and the outbound counter never
             moved, so MQTTD's No Local made the cut
  ok   — 5. outbound: one publish, exactly one delivery on the incumbent, and the inbound
             counter never moved — the FOREIGN broker honoured No Local
  ok   — 6. a retained value pruned with the bridge UP is gone on BOTH sides (the
             tombstone crossed)
  ok   — every fss_bridge_* series the playbook's verification table cites exists,
             anchored and labelled
DUAL RUN SMOKE OK
```

That is the script's own output, copied from a run on the date above (twice in a row, same
result; the ports differ per run because it binds ephemeral ones). Four `ok` lines are
wrapped here for page width and are single lines on a terminal — nothing else is edited.

**Which assertion tests the foreign broker — corrected here, because the earlier version of
this paragraph got it backwards.** It used to say assertion 4 was "a claim about Mosquitto
2.1.2's behaviour". It is not. Assertion 4 publishes on the incumbent and counts on mqttd,
and the bridge's own counters show what actually happened:
`fss_bridge_forwarded_total{direction="in"}` moves by one while `{direction="out"}` does not
— the message never travelled back toward Mosquitto, because **mqttd** declined to echo it
to the bridge's own subscription. The loop was cut on the mqttd side, before Mosquitto's No
Local behaviour was ever consulted.

**Assertion 5 is the one that tests the foreign broker**, and it was added for exactly that
reason: publish on mqttd, *count* on the incumbent. Mosquitto delivers to the test
subscriber and would also deliver back to the bridge's incumbent-side subscription if it
ignored No Local — which would return to mqttd, be forwarded out again, and land a second
copy. A count of exactly 1 there, with `{direction="in"}` unmoved, **is** a claim about
Mosquitto 2.1.2. Both assertions check their counter pair, so the attribution is measured
rather than asserted.

**Untested, stated plainly:**

- **The bridge step was verified against Mosquitto 2.1.2, not against EMQX or
  HiveMQ.** Both document the same No Local behaviour — EMQX's own docs describe
  `bridge_mode` as "equivalent to No Local = 1" — and both speak MQTT 5, so the
  hop-count user property survives. Neither was run. Assertion 5 makes the third-party
  claim real *for Mosquitto*; it extends to EMQX and HiveMQ only as far as their
  documentation does, which is not the same as a measurement.
- **No converter has ever been fed a config produced by a live EMQX or HiveMQ
  broker.** The fixtures are vendor *source* at pinned tags — files fetched from the
  vendor's repository, not configs dumped out of a running instance. Nothing on this page
  should be read as validation against a live EMQX or HiveMQ, because there was none.
- **The version ranges are a parser claim, not a fidelity claim.** Only one tag per broker
  is a fixture and in CI, and for Mosquitto not even that. EMQX `v5.8.9`'s and HiveMQ CE `2023.10`/`2024.9`'s shipped
  examples were converted by hand once (exit 0, `--check-config` passes — see
  [provenance](#honesty-about-provenance)); nothing checks that any *mapping* means the
  same thing in those versions, and nothing re-checks it on future commits.
- **The `keytool` half of the JKS extraction recipe was not run** (no Java runtime). Its
  `openssl` half was, in the exact form printed above: a locally minted PKCS#12 through
  `openssl pkcs12 -nokeys` / `-nocerts -nodes` / `openssl pkey`, then mqttd booted on the
  extracted pair and logged `accepting MQTT 3.1.1 clients over TLS 1.3` — bag-attribute
  preamble in the certificate file included, which rustls skips.
- **Steps 0, 1, 2, 4 and 6 are procedure, not a harness.** The commands in them were
  each run in isolation here (`cert-audit.sh`, the converters, `--check-config`,
  `--probe`), but no test walks a whole cohort migration end to end.

---

## Running the converters and their tests

```sh
cargo build                                      # the tests boot the real binaries
MQTTD_BIN=target/debug/mqttd ./scripts/migrate/test-from-emqx.sh
MQTTD_BIN=target/debug/mqttd ./scripts/migrate/test-from-hivemq.sh
MQTTD_BIN=target/debug/mqttd ./scripts/migrate/test-from-mosquitto.sh
MQTTD_BIN=target/debug/mqttd ./scripts/migrate/dual-run-smoke.sh          # needs the mosquitto BROKER
MQTTD_BIN=target/debug/mqttd ./scripts/migrate/differential-mosquitto.sh  # needs the mosquitto BROKER
```

**All five scripts run in per-PR CI** — the three converter tests, the dual-run smoke,
and the differential lane (`.github/workflows/ci.yml` installs the Mosquitto broker for
the smoke, and the differential step reuses it). The differential lane also runs
anywhere the Mosquitto broker is installed. Each of the five exits **2** — not 1 — when a binary or the Mosquitto broker
is missing, so "environment not ready" is never mistaken for "assertion failed"; verified
by running them with `MQTTD_BIN` pointed at nothing, and the broker-needing ones with
`mosquitto` off the `PATH`.

The converters themselves use the same three-way scheme: **0** with TODOs (a conversion
with gaps is a successful conversion — the gaps are in the file), **1** for input that
could not be read at all, and they never exit 2. So `grep -c 'TODO(migrate)'` and not the
exit status is how you find out how much work is left.

**What the differential lane actually measured.** Executed 2026-08-17 on macOS 14
(Darwin 23.5.0) with **mqttd 0.9.0** (tree build) against **mosquitto 2.1.2** as the
oracle — the source config booted on the vendor, the converted config booted on mqttd
(finished exactly as its own TODOs instruct: re-hashed credentials, the ACL path, a real
data dir), the same client binaries probing both:

```
versions under test:
  vendor oracle: mosquitto version 2.1.2
  mqttd:         mqttd 0.9.0
  ok   — vendor verdicts recorded (mosquitto version 2.1.2), and the harness anchors hold
  ok   — mqttd verdicts recorded, booted from the converted config + its own finishing steps
         anonymous-connect      REFUSED
         wrong-password         REFUSED
         valid-credential       ACCEPTED
         permitted-publish      DELIVERED
         publish-outside-acl    NOT_DELIVERED
         permitted-subscribe    DELIVERED
         subscribe-outside-acl  NOT_DELIVERED
  ok   — all 7 verdicts identical: the converted config means what the source config meant
DIFFERENTIAL OK
```

The lane was mutation-proved before it was trusted: a broker booted with anonymous access
switched on records `anonymous-connect ACCEPTED` and the diff fails, and a hand-widened
ACL (`sensor-1` allowed to publish everywhere) records `publish-outside-acl DELIVERED`
and the diff fails. Seven probes over one config is a probe of the auth/ACL/bind
mappings, **not** coverage of `mosquitto.conf(5)` — the [Verified, and not
verified](#verified-and-not-verified) section carries the same caveat.

`mqttui` lists all of them (`mqttui --list`), and the converters travel inside the
`mqttui` binary, so they run with no clone at all.
