# The rule engine, live: watch the rules run, and change them while they do

[`demo/rules`](../rules/README.md) plays ten simulated minutes of a Danish utility and its
fleet into mqttd and stops. This stack keeps them playing, and opens the broker up so you
can watch what its [rule engine](../../docs/RULES.md) does with each message, and change
the rules while it runs ([ADR 0084](../../docs/adr/0084-watching-and-editing-rules-live.md)):

- **mqttd**, built from this checkout, running the demo's 21 rules. Every 2 s it publishes
  each rule's statistics on `$SYS/brokers/<node>/rules/<id>`, and with the rule trace on it
  publishes what each rule ran on and what it rendered on
  `$SYS/brokers/<node>/trace/rules/<id>`.
- **Three simulators**, power plants, homes and cars
  ([`live.py`](../rules/live.py)), playing the demo's ten minutes over and over on the wall
  clock: wind turbines, SunSpec inverters, DSMR smart meters, heat pumps, EV chargers,
  telematics and OBD-II frames, about four messages a second.
- **A rule editor** at <http://localhost:8070>: the rules with their live counts, each
  rule's trace, the live messages, and an editor that checks, tests and applies a rule
  through the broker's admin API, then shows the new rules arriving on `$SYS`.
- **Your own MQTT client** on `localhost:1883`: every topic the page shows, you can
  subscribe to.

## Run it

You need Docker with Compose v2 (`docker compose`). The first run builds mqttd from source
in release mode, which takes 5-15 minutes; later runs reuse Docker's build cache and start
in seconds.

```sh
demo/rules-live/up.sh
```

With `mqttui`, run it as `mqttui --run demo-rules-live`. `up.sh` checks that the host ports
are free, runs `docker compose up --build -d` (always building, so the image matches the
checkout: an older image would ignore the statistics settings without a word), waits until
the broker is healthy, and prints the addresses and the topics below.

| What | Address | Moved with |
|---|---|---|
| The rule editor | <http://localhost:8070> | `UI_PORT` |
| MQTT, plaintext, anonymous | `127.0.0.1:1883` | `MQTT_PORT`, `MQTT_BIND` |
| Health and Prometheus metrics | <http://localhost:8080/metrics> | `HEALTH_PORT` |
| The admin API, mTLS | `https://localhost:9443` | `ADMIN_PORT` |

The broker's node id is `node-local`, so its statistics are on `$SYS/brokers/node-local/…`.

## Watch it with your own MQTT client

A `#` filter never matches a topic that starts with `$` (MQTT 5 §4.7.2), so
`mosquitto_sub -t '#'` shows the devices and what the rules derive, never the statistics.
Subscribe to those by name:

| Topics | What | Try |
|---|---|---|
| `$SYS/brokers/+/rules/#` | A summary every 2 s (the running rules' digest, how many are enabled, the last reload, the trace setting), then one message per rule: its counts since the broker started (`matched`, `passed`, `no_result`, `failed`, `actions_ok`, `actions_failed`), the time spent evaluating it (`eval_ns`, and `eval_us_avg` per message), rates per second, when it last matched and its last error | `mosquitto_sub -v -t '$SYS/brokers/+/rules/#'` |
| `$SYS/brokers/+/trace/rules/+` | The rule trace: for each rule, at most 5 records a second, and 5 more for messages its WHERE turned away (`no_result`), of the message it ran on (topic, client id, the first 1 KiB of the payload), the result, and every message it rendered. Not under `rules/#`: a trace subscription must be asked for on purpose | `mosquitto_sub -v -t '$SYS/brokers/+/trace/rules/+'` |
| `alerts/#`, `kpi/#`, `normalized/#`, `analytics/#`, `state/#`, `events/#` | What the rules derive: the six roots of [the demo's rules](../rules/README.md#where-the-results-go) | `mosquitto_sub -v -t 'alerts/#' -t 'kpi/#' -t 'state/#'` |
| `plant/#`, `home/#`, `vehicle/#` | What the devices publish | `mosquitto_sub -v -t 'vehicle/#'` |

One rule's statistics and trace only, here the grid-frequency alert:

```sh
mosquitto_sub -v -t '$SYS/brokers/+/rules/power_grid_frequency' \
  -t '$SYS/brokers/+/trace/rules/power_grid_frequency'
```

## Change a rule in the editor

Open <http://localhost:8070>. The header shows the digest of the rules that run and of the
file on disk, how many rules are enabled, the last reload and whether the trace is on; the
table below it counts every rule's messages as they arrive, with rates over the last 10 s.

1. Press **New rule**. The editor fills with a small example, `demo_rule_1`. It takes the
   wind farm's grid meter (`plant/+/poc/grid`, a reading every 2 s) and republishes a
   reading to `kpi/demo/<site>/frequency` when the frequency is more than 20 mHz off 50 Hz:
   `payload.Hz < 49.98 OR payload.Hz > 50.02`.
2. **Check** splices the rule into the rules file and validates the whole file, writing
   nothing. An error names the line and column, and selects it in the editor.
3. **Test** runs the rule, as if enabled, on the latest grid reading the page has seen, and
   shows what it would publish, publishing nothing. Most readings are within 20 mHz, so
   the answer is often `no_result`: FROM matched, but WHERE was false.
4. **Apply** writes the rules file (atomically; the previous file is kept as
   `rules.toml.prev` beside it) and reloads it. The answer shows the new digest; a few
   seconds later the summary on `$SYS` reports it, the header changes with it, and
   `demo_rule_1` is in the table. It passes a reading every 7 s or so, in bursts: watch
   its **Passed** and **Passed/s**, its trace (each `passed` record with the message it
   published), and **Live messages** with the filter `kpi/demo/#`.
5. Make it fire less often: in the SQL, change `49.98` to `49.97` and `50.02` to `50.03`
   (and the description to 30 mHz), and **Apply** again. It now passes about a third as
   many readings, one every 20 s or so. **Passed** still grows, since the counts add up from
   the broker's start (an edited rule keeps its counts), but more slowly; **Passed/s**
   drops, and in the trace (tick **Hide no_result**) the passes come further apart.
   **Avg µs** is what the rule costs per message over the last 10 s: its SQL plus
   rendering its actions (not routing what it republishes). Compare rules side by side,
   or watch it while you edit one: a heavier `WHERE` or a `FOREACH` shows up there first.
6. **Delete** removes `demo_rule_1` again.

The simulated grid also has an excursion: 6 min 45 s into every ten-minute window (at
hh:06:45, hh:16:45 and so on, UTC), the frequency drops to about 49.75 Hz and stays low
until the window ends. Meanwhile `demo_rule_1` passes every reading, whatever its band, and
its **Passed** grows faster. The shipped **power_grid_frequency** alerts only outside
49.9-50.1 Hz, so it fires only in the first three minutes or so of the excursion. To see
what it publishes at another time, choose it, choose **this message** under "Test it
against", and **Test** it with the topic `plant/wf-falster/poc/grid` and this payload:

```json
{"ts":1791469801000,"Hz":49.8,"ROCOF":-0.03,"U_kV":{"L12":51.4,"L23":51.3,"L31":51.4}}
```

It passes, and renders the alert it would publish on `alerts/power/grid/frequency`: band
"FCR-D upward", 25 % FCR-D activation.

Rules do not chain: a rule never runs on a message a rule published, its own or another's.
A rule with `FROM "kpi/demo/#"` would never see `demo_rule_1`'s output, only what clients
publish there.

**Delete** removes the chosen rule, and **The whole rules file** at the bottom edits the
file as text, with **Reset to the shipped rules** to put
[`demo/rules/rules.toml`](../rules/rules.toml) back. Every write names the version of the
file its text came from. If the file changed since (in another tab, say) in a way the
write would undo, the write is refused instead of overwriting that change, and a second
Apply writes over it on purpose. (Reset to the shipped rules is the exception: it replaces
whatever is there.)

The edited rules live in the stack's `rules` volume, never in the checkout, and survive
`docker compose down`.

## The admin API from the host

The rules endpoints are ordinary admin API calls ([ADMIN-API.md](../../docs/ADMIN-API.md),
[ADMIN-CLI.md](../../docs/ADMIN-CLI.md)), so anything the page does, you can do from a
terminal. The `admin` service runs the broker's own CLI with the page's certificate,
`CN=rules-ui`, an operator and a rules writer:

```sh
cd demo/rules-live
docker compose run --rm admin rules           # every rule, its counts and last error
docker compose run --rm admin rules-source    # the rules file as it is on disk
docker compose run --rm admin help            # every verb
```

To edit the file in your own editor, take a copy (outside the checkout) and the digest of
the file you copied:

```sh
work="$(mktemp -d)"
docker compose run --rm -T admin rules-source > "$work/rules.toml"
docker compose run --rm -T admin rules-source --json | jq -r .digest
```

Edit `$work/rules.toml`, then write it back. The `admin` container sees its certificate,
not your files, so the file goes in on standard input. `--if_match` names the digest you
took: if the file changed since, the write is refused (`412 digest-mismatch`) instead of
overwriting that change. `--if_match '*'` replaces whatever is there.

```sh
docker compose run --rm -T admin rules-apply /dev/stdin --if_match <digest> < "$work/rules.toml"
```

The answer shows the new file's `digest`, which the next write names. To remove one rule:

```sh
docker compose run --rm admin rule-delete <id> --if_match <digest>
```

With curl, copy the certificate out of the stack first, somewhere outside the checkout:

```sh
pki="$(mktemp -d)"
docker compose cp ui:/pki/. "$pki"
curl --cacert "$pki/ca.crt" --cert "$pki/rules-ui.crt" --key "$pki/rules-ui.key" \
  https://localhost:9443/admin/v1/rules
```

## Ports

Every port is published on 127.0.0.1. When one is taken (a local mosquitto on 1883, the
[cluster demo](../README.md) on 1883 and 8080), `up.sh` says which, and how to move it:

```sh
MQTT_PORT=1884 UI_PORT=8071 HEALTH_PORT=8081 ADMIN_PORT=9444 demo/rules-live/up.sh
```

Give the same variables to any later `docker compose up` (export them in your shell), or
Compose recreates the broker on the default ports. `docker compose run --rm admin`, `logs`,
`cp` and `down` do not need them.

`MQTT_BIND=0.0.0.0` publishes MQTT on every interface, to reach the stack from a phone or
another machine. That exposes an anonymous broker, with the rule trace, to your network:
only on one you trust.

## Stop and reset

```sh
cd demo/rules-live
docker compose down       # stop; the edited rules and the PKI stay in their volumes
docker compose down -v    # stop and forget them: the next up.sh starts from the shipped rules
```

## What the clock changes

The simulators' devices tell the time by the wall clock, so what they report, and what the
rules make of it, depends on when you watch:

- the solar park produces only in daylight, so its inverter ground fault (which needs 5 kW
  of DC) never fires at night;
- the EV peak-tariff advice fires only for a charging session that starts between 17:00 and
  21:00 Danish time;
- the heat-pump and heating faults happen only in the heating season.

Each ten-minute window also starts the devices afresh: odometers, meter registers and
charge levels jump back, and a faulted turbine is healthy again. The rules see one message
at a time, so this raises no false alert.

To see every alert whatever the hour, give the devices the README's clock: every window
then replays [the demo's ten minutes](../rules/README.md), 54 alerts included.

```sh
SIM_CLOCK=fixture demo/rules-live/up.sh
```

## Security: a demo on your own machine

- **The broker is anonymous, with no ACL file.** Whoever reaches its MQTT port may publish
  and subscribe anywhere, the rule trace included, and the trace copies payloads, client ids
  and user names onto `$SYS`.
- **The editor is an admin-API operator without a login.** It holds the `CN=rules-ui`
  certificate, which may write the rules file, so whoever reaches the page can rewrite the
  rules, and through them read and republish any message. It refuses what another web site
  could send it (it serves only `Host: localhost:<port>` or `127.0.0.1:<port>`, takes a
  write only from its own page, with a JSON body and its own header, and sends a strict
  Content-Security-Policy), and it shows every message as text, never as HTML. It still
  has no password.
- **So every port is on 127.0.0.1, and there is no WebSocket listener**: the broker's
  WebSocket listener has no Origin check, so any web page you visit could reach it.
- The PKI is throwaway, minted on the first run into Docker volumes, never into the
  checkout; the CA key is in a volume only the one-shot `pki` service mounts.

For a real deployment, leave the trace off and `[rules] admin_writers` empty, and grant
`$SYS/brokers/+/rules/#` only to who needs the statistics
([HARDENING.md](../../docs/HARDENING.md)).

## When something is off

- **"This broker predates ADR 0084"** in the editor: the broker has no rules endpoints, or
  publishes no statistics. The image is older than the checkout; `up.sh` rebuilds it.
- **No statistics with your own client:** subscribe to `$SYS/brokers/+/rules/#` by name;
  `#` does not cover it.
- **What each service says:** `docker compose logs -f mqttd` (or `ui`, `sim-cars`, …),
  from `demo/rules-live`.

## Files

| File | What |
|---|---|
| [`compose.yaml`](compose.yaml) | The stack: `pki` and `rules-seed` (one-shot), `mqttd`, `sim-power`, `sim-homes`, `sim-cars`, `ui`, and `admin` (run on demand) |
| [`up.sh`](up.sh) | Preflight the ports, build, start, wait until healthy, print where to look |
| [`pki/gen.sh`](pki/gen.sh) | The throwaway CA, the admin listener's certificate and the `CN=rules-ui` client certificate |
| [`ui/server.py`](ui/server.py) | The editor's server, standard library only: the page, the admin API over mTLS, and MQTT as server-sent events |
| [`ui/index.html`](ui/index.html), [`ui/app.js`](ui/app.js), [`ui/style.css`](ui/style.css) | The page, with no third-party code |

The simulators and the rules are [`demo/rules`](../rules/README.md)'s, mounted read-only.
