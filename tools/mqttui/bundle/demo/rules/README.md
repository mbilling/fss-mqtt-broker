# The rule engine on realistic data: power plants, homes and cars

Raw telemetry is rarely what anyone needs. A wind farm, a street of smart homes and a
service fleet together send thousands of messages in ten minutes. Most of them are normal,
some are binary or in a protocol only one vendor's software reads, and a few contain the
fault that matters. This demo plays ten simulated minutes of a Danish utility and its
fleet into mqttd and shows what its [rule engine](../../docs/RULES.md) makes of them.

- **Decodes** what nobody can read: SunSpec registers, DSMR smart-meter telegrams, a
  legacy RTU's CSV, OBD-II CAN frames, trouble codes.
- **Normalizes** each into one record with units.
- **Picks out** the 54 messages that need a person.
- **Computes** the figures behind them: power-curve performance, heat-pump COP, the cost of
  this minute's draw in the peak tariff.
- **Keeps** the last-known state of the turbines, the peaker, the heat pumps and every
  vehicle's online flag, retained, for a screen that opens late.
- **Pseudonymizes** a feed for a third party.

All of it runs in the broker, as you see it here: 21 rules in
[`rules.toml`](rules.toml), no code.

```text
2362 device messages in, 1333 derived out (alerts 54, analytics 159, events 22, kpi 140, normalized 769, state 189)
```

Every one of the 54 alerts is one of the faults the simulation injects; the normal data
around them raises none.

## Run it

```sh
demo/rules/run.sh
```

It starts mqttd on free localhost ports with `rules.toml` and plays this page's ten minutes
at ten times real time (about a minute): the numbers and alerts below are what it prints. It prints each device message (`→`) and each message
the rules derive (`⇒`), then the broker's own per-rule counters. It needs Python 3
(standard library only) and a build of mqttd with the rule engine: no release has it yet,
so `run.sh` uses this checkout's `target/` build, or runs `cargo build --release -p mqttd`
the first time. With `mqttui`, run it as `mqttui --run demo-rules`.

Every option goes to the simulator:

```sh
demo/rules/run.sh --speed 1                  # real time: ten minutes
demo/rules/run.sh --domains cars --quiet     # one domain, derived messages only
demo/rules/run.sh --start now --duration 3600   # an hour from now: the sun, the tariff and
                                                # the faults follow the real clock instead
demo/rules/run.sh --help                     # everything else
```

To watch it with your own tools, run the broker yourself, from a build that has the rule
engine ([RULES.md, step 1](../../docs/RULES.md#1-get-a-build-that-has-the-rule-engine); a
released `mqttd` would ignore the rules file without a word). In one terminal:

```sh
cargo build --release -p mqttd
MQTTD_PLAINTEXT_BIND=127.0.0.1:1884 MQTTD_ALLOW_ANONYMOUS=1 MQTTD_DURABLE_SESSIONS=0 \
  MQTTD_RULES_FILE=demo/rules/rules.toml target/release/mqttd
```

Its log must say `rule engine: rules loaded (ADR 0083) rules=21`. Then, in another:

```sh
mosquitto_sub -p 1884 -t 'alerts/#' -v &
python3 demo/rules/simulate.py --port 1884 --no-watch --quiet
```

## What is simulated

The simulation is seeded: the same `--seed`, `--start` and `--duration` give the same
messages, byte for byte. The numbers on this page come from the demo's fixture, which is
also what `run.sh` plays by default: `--seed 7 --start 2026-03-24T15:55:00Z --duration 600`.
That is 16:55 to 17:05 Danish time on a Tuesday in late March: late afternoon with the sun
low and falling, heating on, and the grid tariff's evening peak starting five minutes in.
Each device publishes the way its real equipment does, and each domain's module documents
its formats, physics and injected faults in detail:
[`power.py`](sim/power.py), [`homes.py`](sim/homes.py), [`cars.py`](sim/cars.py).

| Domain | Devices | Formats | Messages in 10 min |
|---|---|---|---|
| Power plants | 6 wind turbines and their park controller, 4 solar inverters, a gas peaker, a grid meter | IEC 61400-25 style JSON, SunSpec model 103 registers with scale factors, a legacy RTU's CSV line, a PMU's frequency and ROCOF | 790 |
| Homes | 12 households: smart meters, thermostats, heat pumps, rooftop PV, batteries, EV chargers | DSMR 5.0 P1 telegrams (text, OBIS codes, CRC), JSON, OCPP 1.6J | 823 |
| Cars | 9 service-fleet vehicles: electric cars and vans, diesel vans | telematics JSON, a buffered trip log, raw OBD-II Mode 01 and Mode 03 CAN responses (binary); ignition on and off as MQTT connects and disconnects | 749 |

The injected faults, in the order the rules first report them (seconds after
15:55:00 UTC, by the device's clock):

| s | Fault | Reported by |
|---|---|---|
| 24 | Home hh-117's circulation pump is failing: its heat pump keeps tripping, and the living room has drifted 2.1 K under its setpoint | `home_heating_comfort` |
| 88 | hh-117's heat pump locks out on a high-pressure fault | `home_heatpump_health` |
| 109 | Inverter INV03 trips on a ground fault (at 96 s; it reports FAULT from 109 s) | `power_pv_fault` |
| 141 | An electric car loses its connection without a goodbye; it comes back at 305 s and uploads its trip log | `car_presence`, `car_mobility_feed` |
| 174 | An electric van brakes at 0.56 g | `car_driving_events` |
| 180 | Turbine WTG05's blade pitch is 4° off optimum (from 125 s): 75-79 % of its power curve, while it reports "producing" | `power_wtg_performance` (a KPI, not an alert) |
| 215 | A diesel van stores trouble code P0301 (cylinder 1 misfire) | `car_obd_dtc` |
| 240 | Turbine WTG06's gearbox starts to fail (from 150 s): vibration in ISO 10816-21 zone C, then D; at 372 s the turbine stops on its vibration trip | `power_wtg_condition`, `power_wtg_state`, `power_wtg_performance` |
| 301 | Home hh-142, at the end of a rural feeder, sees its L2 phase fall to 205 V (from 290 s) | `home_voltage_en50160` |
| 304 | Home hh-104's EV starts charging as the 17:00 peak tariff starts (plugged in at 281 s) | `home_ev_peak_start`, `home_tariff_rate` |
| 316 | Home hh-123's heat pump runs below a COP of 2.0 in mild weather (an iced evaporator, from 195 s: its retained state says so at once, the alert comes at the next 5-minute mark) | `home_heatpump_health` |
| 330 | Another diesel van stores P0128 (thermostat stuck open) | `car_obd_dtc` |
| 355 | A third van's coolant passes 105 °C, and 115 °C at 485 s, when its engine ECU stores P0217 | `car_engine_overheat`, `car_obd_dtc` |
| 360 | Turbine WTG03's gearbox oil cooler fan failed at 5 s: the oil reaches 75 °C, then 80 °C, and at 82 °C (496 s) the turbine derates itself | `power_wtg_condition`, `power_wtg_performance`, `power_wtg_state` |
| 402 | A car does 67 km/h in a 40 zone | `car_driving_events` |
| 405 | A large generator trips elsewhere in the Nordic grid (403 s): frequency falls to 49.749 Hz | `power_grid_frequency` |
| 421 | An electric car's pack passes 50 °C while DC fast charging | `car_ev_battery` |
| 483 | An electric van drops below 15 % charge | `car_ev_battery` |

## Where the results go

Every rule publishes under one of six roots, then the domain, so a consumer subscribes to
exactly the kind of data it wants:

| Root | What | QoS | In 10 min |
|---|---|---|---|
| `alerts/<domain>/…` | an exception a person should act on, most with what to do | 1 | 54 |
| `kpi/<domain>/…` | a computed figure: performance, cost | 0 | 140 |
| `normalized/<domain>/…` | a decoded, unit-normalized canonical record | 0 | 769 |
| `analytics/<domain>/…` | a privacy-safe feed for third parties | 0 | 159 |
| `state/<domain>/…` | the last-known state, retained | 1 | 189 |
| `events/<domain>/…` | presence and lifecycle events | 1 | 22 |

A retained `state/` message reaches a subscriber that connects later with the retain flag
set. A subscriber already connected receives it as an ordinary message, as MQTT
specifies.

## The rules

Each example below is a real message from the fixture (`→`, shortened where marked) and
what the broker derived from it (`⇒`). The test suite replays the fixture through mqttd
and checks every `⇒` line on this page.

### Power plants

**`power_pv_sunspec`: registers to engineering units.** A SunSpec inverter reports
integers plus a scale factor per value: `"W": 5514` with `"W_SF": 2` is 551.4 kW. Nothing
downstream should have to know that. The rule scales every value once and names the
operating state. It turns "not implemented" sentinels into `null` instead of
-3276.8 °C, and adds the DC-to-AC efficiency, the inverter's health figure.

```text
→ plant/pv-lolland/inv02/sunspec  {"ts":1774367711500,"model":103,"A":4935,"A_SF":-1,"PPVphAB":6452,"PPVphBC":6452,"PPVphCA":6451,"V_SF":-1,"W":5514,"W_SF":2,"Hz":4999,"Hz_SF":-2,"VA":5514,"VA_SF":2,"VAr":0,"VAr_SF":2,"PF":10000,"PF_SF":-2,"WH":17787843,"WH_SF":3,"DCA":5002,"DCA_SF":-1,"DCV":11268,"DCV_SF":-1,"DCW":5636,"DCW_SF":2,"TmpCab":210,"TmpSnk":285,"TmpTrns":-32768,"TmpOt":-32768,"Tmp_SF":-1,"St":4,"StVnd":4001,"Evt1":0,"Evt2":0,"EvtVnd1":0}
⇒ normalized/power/pv-lolland/inv02  {"site":"pv-lolland","inverter":"inv02","at":"2026-03-24T15:55:11Z","state":"MPPT","ac_kw":551.4,"ac_kvar":0.0,"pf":1.0,"ac_v":645.2,"ac_a":493.5,"ac_hz":49.99,"dc_v":1126.8,"dc_a":500.2,"dc_kw":563.6,"efficiency_pct":97.8,"energy_kwh":17787843,"cabinet_c":21.0,"heatsink_c":28.5}
```

**`power_pv_fault`: a fault, by name, with the evidence.** Only an inverter in state 7
(FAULT) produces anything. `Evt1: 1` becomes `GROUND_FAULT`, alongside the array's DC
voltage, which tells the operator to send a technician with an insulation tester.

```text
→ plant/pv-lolland/inv03/sunspec  {"ts":1774367809000, … "St":7,"StVnd":7104,"Evt1":1,"Evt2":0,"EvtVnd1":4096}   (shortened)
⇒ alerts/power/pv-lolland/inv03/fault  {"site":"pv-lolland","inverter":"inv03","at":"2026-03-24T15:56:49Z","state":"FAULT","events":["GROUND_FAULT"],"evt1":1,"vendor_event":4096,"vendor_state":7104,"dc_v":1358.9}
```

**`power_wtg_condition`: a minute's batch, down to the turbines that need a look.** The
park controller sends one message a minute holding every turbine's means. `FOREACH … INCASE`
fans it out and keeps only gearbox vibration in ISO 10816-21 zone C or D, or gearbox oil
at 75 °C or more. Ten batches, 60 turbine-minutes: seven alerts, each with a severity and
an action.

```text
→ plant/wf-falster/ppc/stats  {"ts":1774368060000,"site":"wf-falster","period_s":60,"P_kW":12532.6, … {"id":"wtg03", … "TmpGbxOil":76.5, … "VibGbx":1.7}, … {"id":"wtg06", … "TmpGbxOil":57.8,"TmpStat":74.2,"VibGbx":5.81}]}   (shortened)
⇒ alerts/power/wf-falster/wtg06/condition  {"site":"wf-falster","turbine":"wtg06","period_end":"2026-03-24T16:01:00Z","severity":"critical","gearbox_oil_c":57.8,"gearbox_oil":"normal","vibration_mm_s":5.81,"vibration_zone":"D","action":"damage likely: stop the turbine and inspect the drivetrain before restarting"}
⇒ alerts/power/wf-falster/wtg03/condition  {"site":"wf-falster","turbine":"wtg03","period_end":"2026-03-24T16:01:00Z","severity":"warning","gearbox_oil_c":76.5,"gearbox_oil":"warning","vibration_mm_s":1.7,"vibration_zone":"A/B","action":"check the oil cooler, its fan and the filter today"}
```

**`power_wtg_performance`: the loss a status code hides.** From the same batch, each
turbine-minute becomes a KPI: the power its power curve promises at that wind speed, the
ratio it delivered, and the energy lost. WTG05 reports "producing" all along, yet its
pitch fault costs it 8 to 11 kWh a minute.

```text
⇒ kpi/power/wf-falster/wtg05/performance  {"site":"wf-falster","turbine":"wtg05","period_end":"2026-03-24T15:59:00Z","wind_ms":10.01,"power_kw":2082.0,"expected_kw":2765,"performance_ratio":0.75,"status":"underperforming","lost_kwh":11.4}
⇒ kpi/power/wf-falster/wtg03/performance  {"site":"wf-falster","turbine":"wtg03","period_end":"2026-03-24T16:04:00Z","wind_ms":8.61,"power_kw":1430.0,"expected_kw":1886,"performance_ratio":0.76,"status":"derated","lost_kwh":7.6}
```

**`power_grid_frequency`: the grid event, classified.** The PMU at the 50 kV connection
reports every 2 s. Only readings outside the Nordic normal band (49.9-50.1 Hz) produce an
alert: every reading while the frequency is still moving fast (the onset of a disturbance)
or outside 49.5-50.5 Hz, otherwise one every 10 s while the excursion lasts. Each is
classified by reserve band, with the FCR-D activation it calls for.

```text
→ plant/wf-falster/poc/grid  {"ts":1774368111000,"Hz":49.749,"ROCOF":-0.001,"U_kV":{"L12":51.32,"L23":51.28,"L31":51.35},"P_MW":11.86,"Q_Mvar":-0.08}
⇒ alerts/power/grid/frequency  {"measured_at":"wf-falster/poc","at":"2026-03-24T16:01:51Z","hz":49.749,"deviation_mhz":-251,"rocof_hz_s":-0.001,"band":"FCR-D upward","fcr_d_activation_pct":38,"severity":"warning","u_kv":51.32}
```

**`power_rtu_csv`: a CSV line, named and typed.** The peaker's old RTU sends
`ts,unit,MW,MVAr,kV,Hz,breaker`. The rule names every field, turns empty fields into
`null` and the IEC 60870-5 breaker double point into `open`/`closed`, and computes the
power factor. It adds Danish local time and the tariff period, and keeps the last reading
retained under `state/`.

```text
→ plant/pk-koge/g1/rtu  1774368002,G1,1.6,1.6,10.53,49.983,2
⇒ normalized/power/pk-koge/g1  {"at":"2026-03-24T16:00:02Z","local_time":"2026-03-24T17:00:02+01:00","load_period":"peak","unit":"G1","p_mw":1.6,"q_mvar":1.6,"u_kv":10.53,"hz":49.983,"breaker":"closed","pf":0.707}
```

**`power_wtg_state`: state by exception.** Each turbine reports every 10 s. Its state is
republished, retained, only when it changed, plus a periodic integrity report every 10
minutes. A control-room screen that opens late reads the current state of every turbine
at once.

```text
→ plant/wf-falster/wtg03/tele  {"ts":1774368203900,"WTUR":{"TurSt":{"stVal":4,"t":1774368196370},"AlmCd":2310,"W":1467.3,"VAr":-18.0}, … "WTRM":{"TmpGbxOil":82.2,"VibGbx":1.36}, …}   (shortened)
⇒ state/power/wf-falster/wtg03  {"site":"wf-falster","turbine":"wtg03","at":"2026-03-24T16:03:23Z","state":"derated","since":"2026-03-24T16:03:16Z","alarm":"gearbox oil temperature high: power limited","report":"change"}
```

### Homes

**`home_p1_decode`: the smart meter's telegram, decoded.** A DSMR 5.0 meter's P1 port
sends text: OBIS codes, units in brackets, a local clock with a summer/winter flag, and a
CRC. The rule turns it into numbers with units, a UTC time, and the net power the home
draws from the grid.

```text
→ home/hh-104/p1  /XMX5LGBBLB0908932596
                  0-0:1.0.0(260324165505W)
                  1-0:1.8.1(004425.887*kWh)   1-0:1.8.2(005565.235*kWh)
                  1-0:2.8.1(001673.297*kWh)   1-0:2.8.2(003682.961*kWh)
                  1-0:1.7.0(00.077*kW)        1-0:2.7.0(00.000*kW)
                  1-0:32.7.0(235.9*V)   1-0:52.7.0(234.5*V)   1-0:72.7.0(235.9*V)   …
                  !…                          (shortened; the lines are CRLF-separated)
⇒ normalized/homes/hh-104/meter  {"home":"hh-104","ts":"2026-03-24T15:55:05Z","meter_id":"E0096964814095259","import_kwh":9991.122,"export_kwh":5356.258,"import_kw":0.077,"export_kw":0.0,"net_kw":0.077,"voltage_v":[235.9,234.5,235.9],"current_a":[0,0,0]}
```

**`home_tariff_rate`: what this minute costs.** Once a minute per meter, the rule works out
the Danish grid-tariff period (Tarifmodel 3.0: low, high, peak 17-21) from the meter's own
clock, and what the current draw costs per hour. At 17:01 hh-104's car is charging at
11 kW in the peak: 15 DKK an hour in tariff alone.

```text
⇒ kpi/homes/hh-104/tariff  {"home":"hh-104","local_time":"2026-03-24T16:55:05+01:00","period":"high","until":"17:00","tariff_dkk_kwh":0.45,"import_kw":0.077,"cost_dkk_h":0.03}
⇒ kpi/homes/hh-104/tariff  {"home":"hh-104","local_time":"2026-03-24T17:01:05+01:00","period":"peak","until":"21:00","tariff_dkk_kwh":1.35,"import_kw":11.075,"cost_dkk_h":14.95}
```

**`home_ev_peak_start`: advice while it still helps.** When a charging session starts
between 17:00 and 21:00 (OCPP `StartTransaction`), the owner hears what moving it saves.

```text
→ home/hh-104/evcharger  {"action":"StartTransaction","connectorId":1,"idTag":"04E1A35A7C6B80","meterStart":6184220,"timestamp":"2026-03-24T16:00:04.500Z"}
⇒ alerts/homes/hh-104/ev-charging  {"home":"hh-104","ts":"2026-03-24T16:00:04.500Z","local_time":"2026-03-24T17:00:04+01:00","tariff":"peak 17:00-21:00","saving_dkk_per_kwh":1.2,"advice":"Charging started in the peak tariff. Postpone it to after 21:00, or to 00:00-06:00 for the lowest grid tariff."}
```

**`home_heatpump_health`: health, not just numbers.** Every heat-pump reading becomes a
retained health state: ok, a fault, a low COP (heat out ÷ electricity in) below 2.0 in
mild weather, or a bad reading (the compressor runs but no power is metered). It carries
what the inefficiency costs. A fault is also an alert at once and then every 5 minutes; a
low COP, at the next 5-minute mark.

```text
→ home/hh-123/heatpump  {"ts":1774368016349,"mode":"heat","compressor":true,"compressor_hz":83,"elec_w":1248,"heat_w":2142,"flow_c":33.3,"return_c":31.5,"flow_lpm":18.1,"outdoor_c":5.8,"defrost":false,"fault":"","fault_since":null}
⇒ alerts/homes/hh-123/heatpump  {"home":"hh-123","ts":"2026-03-24T16:00:16Z","health":"low COP","mode":"heat","compressor":true,"defrost":false,"fault":"","fault_since":"","cop":1.72,"heat_w":2142,"elec_w":1248,"outdoor_c":5.8,"flow_c":33.3,"excess_w":636,"excess_kwh_day":15.3,"advice":"COP below 2.0 in mild weather: check for an iced evaporator or low refrigerant"}
⇒ alerts/homes/hh-117/heatpump  {"home":"hh-117","ts":"2026-03-24T15:56:28Z","health":"fault","mode":"heat","compressor":false,"defrost":false,"fault":"E35 high-pressure lockout","fault_since":"2026-03-24T15:56:28Z","cop":0.0,"heat_w":0,"elec_w":38,"outdoor_c":5.5,"flow_c":41.0,"excess_w":0,"excess_kwh_day":0.0,"advice":"the heat pump has stopped on a fault: give the installer the code"}
```

**`home_heating_comfort`: the room, not the machine.** A room in heat mode more than 2 K
under a setpoint it has had for at least 3 hours means the heating is failing (with a
21.5 °C setpoint, that is already below EN 16798-1's 20 °C). The wait keeps the recovery
from a night setback quiet. It alerts at most every 5 minutes, and works for
district-heated homes too, where the thermostat is the only signal.

```text
⇒ alerts/homes/hh-117/heating  {"home":"hh-117","ts":"2026-03-24T15:55:24Z","temp_c":19.4,"setpoint_c":21.5,"setpoint_since":"2025-10-01T16:00:00Z","shortfall_k":2.1,"problem":"heating not keeping up: check the heat source"}
```

**`home_voltage_en50160`: power quality, out of 8,640 readings a day.** One alert per
phase outside EN 50160's 207-253 V band, at most one a minute. This is evidence for the
grid operator about the end of a weak feeder.

```text
→ home/hh-142/p1  … 1-0:32.7.0(213.3*V) 1-0:52.7.0(205.4*V) 1-0:72.7.0(215.2*V) …   (shortened)
⇒ alerts/homes/hh-142/voltage  {"home":"hh-142","ts":"2026-03-24T16:00:01Z","phase":"L2","voltage_v":205.4,"limit_v":207,"deviation_pct":-10.7,"severity":"warning","basis":"one reading a minute outside the EN 50160 band of 207-253 V, which the standard applies to 10-min means"}
```

**`home_grid_feed`: a feed a third party may have.** For a research partner or a
flexibility aggregator, the rule publishes minute-resolution load under a salted SHA-256
pseudonym. Values are coarsened and carry no meter id, register totals or currents, as
GDPR's pseudonymization and data-minimization principles ask.

```text
⇒ analytics/homes/0a76fdc52a98f337/load  {"id":"0a76fdc52a98f337","minute":"2026-03-24T15:55Z","net_kw":3.9,"voltage_v":[231,232,234]}
```

### Cars

**`car_obd_decode`: CAN frames to engineering units.** The diesel vans' OBD-II dongles
send raw Mode 01 responses: a timestamp, the ECU's id, then PID 0C (engine speed, ÷4),
0D (km/h), 05 (coolant, −40) and 2F (fuel, ×100/255). The rule decodes the bytes with
`bin2hexstr`, `substr` and arithmetic.

```text
→ vehicle/XDKVD2T51NH140928/obd  0x69c2b3d907e8410c19590d1c05822f8a   (binary)
⇒ normalized/cars/XDKVD2T51NH140928/engine  {"vehicle":"XDKVD2T51NH140928","at":"2026-03-24T15:55:05Z","ecu":"7E8","rpm":1622.25,"speed_kmh":28,"coolant_c":90,"fuel_pct":54.1}
```

**`car_obd_dtc`: a trouble code, spelled out.** The two DTC bytes encode a letter (P, C, B
or U) in their top two bits and four digits in the rest. `0x0301` is P0301. The rule
spells it out, says what it means, and says what to do.

```text
→ vehicle/XDKVD2T51NH140928/dtc  0x69c2b4ab07e843010301   (binary)
⇒ alerts/cars/XDKVD2T51NH140928/dtc  {"vehicle":"XDKVD2T51NH140928","at":"2026-03-24T15:58:35Z","local_time":"2026-03-24T16:58:35+01:00","ecu":"7E8","code":"P0301","system":"powertrain","description":"Cylinder 1 misfire detected","severity":"warning","action":"book the workshop today (injector or compression on cylinder 1); avoid heavy loads; if the engine lamp flashes, stop"}
```

**`car_engine_overheat`: the van to call now.** From the same frames, coolant at 105 °C
is a warning and 115 °C is critical, each with an action.

```text
→ vehicle/XDKVD2T52PH151374/obd  0x69c2b5b907e8410c10f00d04059b2f50   (binary)
⇒ alerts/cars/XDKVD2T52PH151374/engine  {"vehicle":"XDKVD2T52PH151374","alert":"coolant over-temperature","severity":"critical","at":"2026-03-24T16:03:05Z","local_time":"2026-03-24T17:03:05+01:00","coolant_c":115,"speed_kmh":4,"rpm":1084,"action":"stop at the kerb now and switch the engine off; do not open the radiator cap hot"}
```

**`car_driving_events`: safety, not surveillance.** Out of a telemetry record every 5 s,
only harsh braking (beyond 0.4 g, from 10 km/h or more) and speeding more than 10 % over
the map-matched limit (`lim`) become events.

```text
→ vehicle/XDKVE6T79SH090233/telemetry  {"ts":1774367874100,"lat":55.673188,"lon":12.557336,"hdg":74,"spd":0.0,"lim":40,"gear":"D","ax":-5.47,"ax_spd":19.5,"soc":63.1,"bat_t":15.2,"odo":51208.9,"chg":"off","chg_kw":0.0}
⇒ alerts/cars/XDKVE6T79SH090233/driving  {"vehicle":"XDKVE6T79SH090233","event":"harsh braking","severity":"warning","at":"2026-03-24T15:57:54Z","local_time":"2026-03-24T16:57:54+01:00","lat":55.67319,"lon":12.55734,"heading":74,"speed_kmh":19.5,"limit_kmh":null,"over_pct":null,"accel_g":-0.56}
→ vehicle/XDKCE5K39PH311806/telemetry  {"ts":1774368107090,"lat":55.685162,"lon":12.574328,"hdg":45,"spd":66.8,"lim":40,"gear":"D","ax":1.63,"ax_spd":49.5,"soc":71.1,"bat_t":13.6,"odo":9383.2,"chg":"off","chg_kw":0.0}
⇒ alerts/cars/XDKCE5K39PH311806/driving  {"vehicle":"XDKCE5K39PH311806","event":"speeding","severity":"critical","at":"2026-03-24T16:01:47Z","local_time":"2026-03-24T17:01:47+01:00","lat":55.68516,"lon":12.57433,"heading":45,"speed_kmh":66.8,"limit_kmh":40,"over_pct":67,"accel_g":0.17}
```

**`car_ev_battery`: charge and heat, in kilometres and actions.** Low charge (below 15 %)
comes with the remaining range and the distance to the depot. A pack over 50 °C while DC
fast charging comes with an action. The van's battery size is read from its VIN.

```text
→ vehicle/XDKVE4T25RH204117/telemetry  {"ts":1774368183080,"lat":55.674449,"lon":12.571961,"hdg":143,"spd":22.5,"lim":40,"gear":"D","ax":-0.33,"ax_spd":23.2,"soc":14.8,"bat_t":17.9,"odo":38415.2,"chg":"off","chg_kw":0.0}
⇒ alerts/cars/XDKVE4T25RH204117/battery  {"vehicle":"XDKVE4T25RH204117","alert":"low charge","severity":"warning","at":"2026-03-24T16:03:03Z","local_time":"2026-03-24T17:03:03+01:00","soc_pct":14.8,"range_km":27,"depot_km":1.9,"pack_temp_c":17.9,"charging":"off","charge_kw":0.0,"action":"finish the current job, then return to the depot to charge"}
⇒ alerts/cars/XDKCE7K30SH118402/battery  {"vehicle":"XDKCE7K30SH118402","alert":"pack overheating while DC charging","severity":"warning","at":"2026-03-24T16:02:01Z","local_time":"2026-03-24T17:02:01+01:00","soc_pct":49.8,"range_km":202,"depot_km":0.6,"pack_temp_c":50.4,"charging":"dc","charge_kw":94.1,"action":"stop the DC session and let the pack cool; book a battery-cooling check before the next fast charge"}
```

**`car_presence`: online, offline, and why.** No device sends this: the broker's own
`$events/client/connected` and `$events/client/disconnected` become a presence event and
a retained online flag per vehicle. The disconnect reason tells "ignition off" from "lost
coverage, may still be moving".

```text
⇒ events/cars/XDKCE5K38RH287145/presence  {"vehicle":"XDKCE5K38RH287145","body":"car","powertrain":"battery-electric","online":false,"reason":"tcp_closed","meaning":"offline: connection lost without a goodbye (coverage, power or modem); the vehicle may still be moving"}
⇒ state/cars/XDKCE5K38RH287145/online  false
```

**`car_mobility_feed`: traffic data the city may have.** For the city's traffic
analytics, live records and the trip log a vehicle uploads after a coverage gap
(`FOREACH`) become 30-second slots. Each slot carries a salted SHA-256 pseudonym instead
of the VIN, a position rounded to about 100 m, and a speed band.

```text
⇒ analytics/cars/mobility  {"id":"0929ff0e6cb95584","class":"car","slot":"2026-03-24T15:57:30Z","lat":55.682,"lon":12.577,"heading":135,"speed_kmh":25,"source":"backfill"}
⇒ analytics/cars/mobility  {"id":"0929ff0e6cb95584","class":"car","slot":"2026-03-24T16:01:30Z","lat":55.675,"lon":12.587,"heading":135,"speed_kmh":25,"source":"live"}
```

## What the numbers say

| | Power plants | Homes | Cars | All |
|---|---|---|---|---|
| Device messages in | 790 | 823 | 749 | 2,362 |
| `normalized/` | 119 | 480 | 170 | 769 |
| `kpi/` | 60 | 80 | | 140 |
| `analytics/` | | 80 | 79 | 159 |
| `state/` (retained) | 47 | 120 | 22 | 189 |
| `events/` | | | 22 | 22 |
| `alerts/` | 25 | 11 | 18 | **54** |

Out of 2,362 messages, 54 need a person, and every one of them is an injected fault. None
comes from the look-alikes the simulation also produces, which a careless threshold would
flag:
- a heat pump's defrost cycle and another's legionella run, where a low COP is normal;
- a degraded heat pump before it locks out (a COP of 2.2 is poor, not failing);
- a 1 K setpoint step, and a cold house in "away" mode;
- EV sessions that began before the peak and run or pause through it (hh-158's meter
  values, hh-131's pause at 17:00);
- healthy turbines and inverters around the faulty ones, and INV03's own low-power restart.

## Reproduce it, and test it

```sh
python3 demo/rules/simulate.py --dry-run --seed 7 --start 2026-03-24T15:55:00Z --duration 600 > /tmp/fixture.jsonl
demo/rules/run.sh --replay /tmp/fixture.jsonl --speed 0
cargo test -p mqttd --test rules_demo
```

The first command writes the fixture, one JSON line per device event; the second plays a
fixture file as fast as the broker takes it. The test generates the same fixture and
replays it through the real mqttd binary with `rules.toml`, one MQTT connection per
client, as the devices and their gateways make them. It asserts that the 1,333 derived messages are exactly
[`rules_demo.expected`](../../crates/mqttd/tests/rules_demo.expected): topic, QoS, retain
flag and payload, with nothing missing and nothing extra. It also checks that the
simulator is deterministic, that `rules.toml` passes `mqttd --check-rules` with no
warning, and that every `⇒` line on this page is one of them. After an intended change to
the rules or the simulator, regenerate the expected output with
`MQTTD_DEMO_BLESS=1 cargo test -p mqttd --test rules_demo the_demo_derives`, and review
its diff like any other change.

## Make it yours

- Thresholds, tariffs and advice text are in the rules' SQL; each rule's comment says
  where its numbers come from. The tariff figures are illustrative: use your grid
  operator's.
- The rules are stateless, like EMQX's: each sees one message. A lasting condition is
  re-alerted on a schedule by the device's own clock: the home rules once a minute or
  every 5 minutes, the coolant and battery warnings once a minute, the grid frequency
  every 10 s. An inverter fault, a speeding record and a critical coolant or battery
  reading alert every time they are reported. A scheduled alert can come that long after
  the condition began, unless the device says when it began, as the heat pump's
  `fault_since` does. Windows and aggregation across messages belong in the consumer.
- Times are formatted from the device's clock with explicit offsets. `format_date` has
  no time-zone rules, so the rules that need Danish summer time read it from the device
  (the P1 meter's summer/winter flag) or compute it from the date. `unix_ts_to_rfc3339`
  would use the broker host's time zone.
- Domain details are in the simulator modules' docstrings and the comments in
  `rules.toml`. Every rule and value in this file is the kind of thing you would write for
  your own fleet, in the [rule SQL](../../docs/RULES.md#sql) EMQX also runs.
