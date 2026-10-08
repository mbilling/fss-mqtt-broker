"""Cars: a Copenhagen field-service fleet's telematics during the evening rush.

Nine vehicles of a Danish utility's service fleet, based at a depot in Kongens Enghave
(Sydhavn, 55.6618 N 12.5528 E), on the streets of central Copenhagen (55.66-55.70 N,
12.54-12.61 E). Metric units. Each vehicle's MQTT client id is its VIN and every device
topic is vehicle/<VIN>/<stream>. Every timestamp is the device's own clock, taken from the
simulation (`ts_ms(t0, at)`), and every random draw comes from the module's seeded
generator, so a seed, a start and a duration give the same bytes on every run. The physics
runs at 1 s resolution. Each vehicle has three random streams of its own, all seeded up
front: its drive (traffic, signal timings), its process noise (engine, charger) and its
sensors' noise. A longer run only draws further along each stream, so the first N seconds
of a run are the same whatever its duration.

The VINs are made up: VIN-shaped (17 characters, no I, O or Q; the ISO 3779 layout, with
the check digit of 49 CFR 565 in position 9) under the invented manufacturer code "XDK",
with this fleet's own descriptor section, which the rules read:

    position  4    body:        C passenger car (M1), V van (N1)
              5    powertrain:  E battery-electric, D diesel
              6    battery (E): 4 = 45 kWh, 5 = 58 kWh, 6 = 68 kWh, 7 = 77 kWh usable
                   engine (D):  2 = 2.0 l
              10   model year:  N 2022, P 2023, R 2024, S 2025

Ignition on and off are MQTT connect and clean DISCONNECT: a vehicle's telematics unit (TCU)
or OBD dongle is online while the ignition is on, or while it charges. A vehicle that is
already driving at the start connects with its first message.

Vehicles moving through traffic follow real streets (polylines along Østerbro, Nørrebro,
Indre By, Vesterbro, Christianshavn, Amager and Sydhavn) with a simple driver model: a
target speed per street, scaled down for the evening rush (never above 50 km/h; 20-35 km/h
is typical at 17:00), traffic signals on fixed 80-100 s cycles that the driver stops for at
1.2-2.6 m/s2 (or crosses on amber when it would need more), and the acceleration of a car
or a loaded van (IDM-style). Position, heading and odometer follow from the speed; the
battery from a road-load model (mass, rolling resistance, drag, 70 % regeneration) plus
cabin heating (6 degC outside). The scenario is written for 10 minutes; a longer run goes
on, with each vehicle parked at the end of its route.

Posted speed limits: each street in the simulation's map carries one. The City of
Copenhagen's "København Ned i Fart" programme makes most streets in Indre By and the inner
districts 30 km/h, zone by zone, and lowers the larger roads between the zones from 50 to
40 km/h (planned for 2024-2026). These routes keep to those larger roads, which the map
has at 40 already; the harbour roads (Kalvebod Brygge, Sydhavnsgade) and Amager Boulevard
are at 50, and the side road into the depot at 30. The values are the simulation's, in the
programme's spirit, not a survey of the signs.

Battery-electric cars and vans: the TCU, JSON every 5 s while online, QoS 1
----------------------------------------------------------------------------
    vehicle/<VIN>/telemetry
      {"ts": ms (TCU clock, GNSS-disciplined),
       "lat", "lon": WGS-84 degrees, 6 decimals (GNSS, about 2 m of correlated error),
       "hdg": course over ground, degrees true, integer (held while standing still),
       "spd": wheel-based speed from the vehicle's CAN bus, km/h, 1 decimal,
       "lim": the posted speed limit where the vehicle is, km/h, from the TCU's map (the
              map-matched limit an Intelligent Speed Assistance system uses; mandatory in
              new EU vehicles since July 2024, Regulation (EU) 2019/2144); null off the
              road network (a car park),
       "gear": "P" (parked) or "D" (drive), the selector position from CAN,
       "ax": longitudinal acceleration, m/s2, + forward: of the five 1-s means of the
             TCU's 100 Hz IMU since the previous record, the one of largest magnitude,
       "ax_spd": the wheel speed at the start of that second, km/h, 1 decimal (so a
             braking is known by the speed it started from),
       "soc": state of charge as the car shows it, %, 1 decimal,
       "bat_t": battery pack temperature (hottest module), degC, 1 decimal,
       "odo": odometer, km, 1 decimal,
       "chg": "off" (not plugged in), "wait" (plugged in, waiting for its schedule),
              "ac" or "dc" (charging),
       "chg_kw": power into the battery, kW, 1 decimal (0 unless charging)}
    e.g. {"ts":1774367701790,"lat":55.686986,"lon":12.564506,"hdg":128,"spd":21.4,"lim":40,
          "gear":"D","ax":1.63,"ax_spd":10.7,"soc":48.6,"bat_t":16.2,"odo":27655.0,
          "chg":"off","chg_kw":0.0}

    vehicle/<VIN>/triplog   once, QoS 1, when a TCU that lost its connection gets it back
      {"ts": upload time, "reason": "backlog", "from": first record's ts, "to": last's,
       "n": number of records, "records": [the 5-s records it buffered, each exactly as
       it would have been sent live]}

Diesel vans: an OBD-II dongle, raw CAN responses as BINARY payloads
--------------------------------------------------------------------
The dongle polls the engine ECU with one multi-PID Mode 01 request (SAE J1979 / ISO 15031-5
over ISO 15765-4 CAN, 11-bit ids) and forwards the reassembled response with its own
6-byte header. All four values in a response are the ECU's at the same instant.

    vehicle/<VIN>/obd     every 10 s while the ignition is on, QoS 0, 16 bytes
      bytes 0-3   device time, Unix seconds, unsigned 32-bit big-endian
      bytes 4-5   CAN id of the answering ECU, big-endian: 0x07E8 = ECU #1 (engine)
      bytes 6-15  41 0C A B 0D A 05 A 2F A: the Mode 01 response (0x40 + 0x01), PID by PID
                    0x0C engine speed   (256 A + B) / 4 rpm
                    0x0D vehicle speed  A km/h
                    0x05 coolant        A - 40 degC
                    0x2F fuel level     100 A / 255 %
      e.g. 69c2b3d9 07e8 410c1959 0d1c 0582 2f8a: 15:55:05 UTC, 1622.25 rpm, 28 km/h,
           90 degC, 54.1 %.

    vehicle/<VIN>/dtc     when the ECU stores a new trouble code, QoS 1
      bytes 0-5   the same header
      bytes 6-    43 N, then N codes of 2 bytes: the Mode 03 response on CAN. A code's
                  first 2 bits are its system (00 P powertrain, 01 C chassis, 10 B body,
                  11 U network), the next 2 its first digit, and the remaining 3 nibbles
                  its last 3 digits (SAE J2012 / ISO 15031-6): 03 01 is P0301.

Warm diesel vans run their coolant at 88-93 degC (the thermostat opens at about 88), shift
up at about 2,100 rpm and idle at 790 rpm. Their dashboard's coolant warning lamp lights at
115 degC, where the engine ECU also stores P0217.

The fleet (VIN, what it is, where it drives)
--------------------------------------------
    XDKVE4T25RH204117  e-van, 45 kWh     from Østerport back to the depot, via Nørreport,
                                         Vester Voldgade and H.C. Andersens Boulevard
    XDKCE7K30SH118402  car, 77 kWh       at a public 150 kW DC charger by Fisketorvet
    XDKVE6T79SH090233  e-van, 68 kWh     along Vesterbrogade, a job on the way (by the
                                         Central Station in the fixture), on towards Langebro
    XDKCE5K39PH311806  car, 58 kWh       from a customer on Nørrebrogade to Østerport
    XDKCE5K38RH287145  car, 58 kWh       Dronning Louises Bro, Gothersgade, Kongens Nytorv,
                                         Knippelsbro and down Amagerbrogade
    XDKCE5K31NH165530  car, 58 kWh       down Enghavevej, back to the depot
    XDKVD2T51NH140928  diesel van        up Amagerbrogade, over Knippelsbro to Kongens Nytorv
    XDKVD2T52PH151374  diesel van        crawling from Jarmers Plads to H.C. Andersens Blvd
    XDKVD2T55RH163019  diesel van        over Langebro and up Vesterbrogade

749 device messages in the 600-s fixture: 575 telemetry records, 1 trip log, 170 OBD
frames and 3 trouble-code frames; plus 3 explicit connects, 3 clean disconnects and 1
dropped connection.

The fixture (--seed 7 --start 2026-03-24T15:55:00Z --duration 600: 16:55-17:05 CET)
-------------------------------------------------------------------------------------
Injected, in seconds after the start. The triggers are the same for any seed; the noise
around them, and so the exact values and some of the times, are not:

  0     XDKCE7K30SH118402 is DC fast charging at 31 % SoC and 134 kW, its pack at 42 degC.
        At 30 s the battery chiller's coolant pump seizes: the pack heats by about 1.3
        degC a minute. Its BMS cuts the charging current from 48.5 degC (at 286 s), which
        slows the rise; the pack passes 50 degC at 391 s and holds at 50-51.5 degC, the
        charger down to 74 kW by the end.
  0     XDKVE4T25RH204117 is at 16.0 % SoC in rush-hour traffic, the heater on; its BMS's
        SoC crosses 15 % at 410 s, and the reading shows 14.9 from about 430 s, near
        Rådhuspladsen, 2 km from the depot.
  40    XDKCE5K39PH311806's ignition comes on (connect) at a customer on Nørrebrogade, the
        selector in P; it drives off at 50 s.
  141   XDKCE5K38RH287145's cellular link fails (a modem stall): the connection is lost
        without a DISCONNECT. The TCU keeps logging every 5 s and reconnects at 305 s,
        uploads the 33 records it buffered as one trip log, then carries on live.
  150   XDKVD2T52PH151374's radiator fan fails in stop-and-go traffic: its coolant climbs
        3-7 degC a minute while it stands and crawls. When the coolant reaches 115 degC
        the dashboard lamp lights and the ECU stores P0217 (the dongle sends it 2 s
        later); the driver pulls in at the next kerb at least 30 m on and switches off 9 s
        after stopping (clean disconnect). Fixture: 105 degC at 356 s, the lamp at 486 s,
        P0217 at 488 s, switched off at 505 s. The dongle always sends at least one frame
        at 115 or more first (the critical level of the coolant rule).
  ~172  XDKVE6T79SH090233 brakes hard for a pedestrian on Vesterbrogade, the first time it
        is at 25 km/h or more after 170 s: about 30 km/h to a stop in 2 s, the second
        second at 5.4 m/s2 (0.55 g), from about 20 km/h.
  ~200  XDKCE5K31NH165530 stops in its bay at the depot (P), is plugged in 20 s later to
        wait for off-peak charging (after 21:00, past the DSO tariff peak) and switches off
        14 s after that (clean disconnect; fixture: 235 s).
  215   XDKVD2T51NH140928's engine ECU stores P0301 (cylinder 1 misfire: a failing
        injector, rough since 170 s); the dongle sends it.
  300   XDKVE6T79SH090233 pulls in at the kerb for a job (P; fixture: by Tivoli and the
        Central Station at 314 s); ignition off 12 s after stopping (clean disconnect), on
        again at 480 s (connect), still in P, and it drives on at 488 s.
  330   XDKVD2T55RH163019's ECU stores P0128 (coolant below the thermostat's regulating
        temperature): its thermostat is stuck open, and the engine has run at 67-72 degC
        all along in the 6 degC air.
  ~400  XDKCE5K39PH311806 speeds along Øster Voldgade (posted 40 km/h), between the
        Botanical Garden and Kongens Have: up to 68 km/h, for about 15 s.

Normal data stays clear of every alert: braking above -2.7 m/s2; speeds at most 92 % of the
posted limit (at most 103 % across seeds 0-119; the alert is at 110 %); SoC above 15 % for
every other EV; no other DC charging; coolant 67-93 degC elsewhere. 18 alerts in all, every
one for an anomaly above: 1 harsh braking, 4 speeding records, 3 trouble codes, 5 coolant
frames (105, 105, 111, then 115 and 116 critical), 3 pack overheating and 2 low charge.
"""

from __future__ import annotations

import math
import random
import struct
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .core import Event, jbytes, ts_ms

# --------------------------------------------------------------------------- geography

M_PER_DEG_LAT = 111_220.0
M_PER_DEG_LON = 111_320.0 * math.cos(math.radians(55.68))  # about 62,750 m

DEPOT = (55.6618, 12.5528)  # the fleet depot in Kongens Enghave
DC_CHARGER = (55.66305, 12.56135)  # a public 150 kW DC charger by Fisketorvet

# Streets as (lat, lon, target km/h of the segment that starts here, traffic signal here,
# posted limit km/h of the segment that starts here). Targets are the speed a driver holds
# in this traffic; every street here is in a built-up area (50 km/h at most).
_OSTERPORT_TO_DEPOT = [
    (55.69270, 12.58680, 38, False, 40),  # Østerport station, heading south-west
    (55.69050, 12.58300, 42, True, 40),   # Øster Voldgade
    (55.68850, 12.57950, 44, True, 40),   # Sølvtorvet (Statens Museum for Kunst)
    (55.68580, 12.57550, 40, True, 40),   # Øster Voldgade by Kongens Have
    (55.68360, 12.57160, 30, True, 40),   # Nørreport
    (55.68170, 12.56900, 36, True, 40),   # Nørre Voldgade
    (55.67970, 12.56630, 32, True, 40),   # Jarmers Plads
    (55.67790, 12.56820, 34, True, 40),   # Vester Voldgade
    (55.67620, 12.56980, 30, True, 40),   # Rådhuspladsen
    (55.67380, 12.57280, 40, True, 40),   # H.C. Andersens Boulevard by the Glyptotek
    (55.67160, 12.57520, 40, True, 40),   # Stormgade / Dantes Plads
    (55.66970, 12.57760, 34, True, 40),   # Langebro, turning onto Kalvebod Brygge
    (55.66800, 12.57400, 46, False, 50),  # Kalvebod Brygge
    (55.66600, 12.56950, 46, True, 50),
    (55.66400, 12.56500, 44, True, 50),
    (55.66290, 12.56220, 38, True, 50),   # Fisketorvet
    (55.66120, 12.55780, 34, True, 50),   # Sydhavnsgade
    (55.66160, 12.55450, 18, False, 30),  # the depot gate
    (55.66180, 12.55280, 0, False, 30),   # the depot
]

_VESTERBRO_TO_AMAGER = [
    (55.67000, 12.54200, 36, False, 40),  # Vesterbrogade, heading east-north-east
    (55.67080, 12.54500, 38, True, 40),   # Enghavevej
    (55.67200, 12.55150, 40, True, 40),   # Vesterbros Torv
    (55.67290, 12.55550, 40, True, 40),   # Frederiksberg Allé
    (55.67370, 12.56050, 42, True, 40),   # Abel Cathrines Gade
    (55.67430, 12.56400, 34, True, 40),   # Bernstorffsgade, by the Central Station and Tivoli
    (55.67520, 12.56720, 28, True, 40),   # Rådhuspladsen
    (55.67380, 12.57280, 34, True, 40),   # H.C. Andersens Boulevard by the Glyptotek
    (55.67160, 12.57520, 38, True, 40),   # Stormgade / Dantes Plads
    (55.66970, 12.57760, 36, True, 40),   # Langebro
    (55.66680, 12.58120, 40, True, 50),   # Amager Boulevard
    (55.66650, 12.58800, 42, True, 50),
    (55.66700, 12.59600, 36, True, 40),   # Christmas Møllers Plads
    (55.66450, 12.60150, 40, True, 40),   # Amagerbrogade
    (55.66150, 12.60380, 40, False, 40),
]

_NORREBRO_TO_OSTERPORT = [
    (55.69370, 12.54980, 34, False, 40),  # a customer on Nørrebrogade, heading south-east
    (55.69260, 12.55210, 38, True, 40),   # Nørrebros Runddel side
    (55.69150, 12.55450, 40, True, 40),   # Blågårdsgade
    (55.68930, 12.55920, 40, True, 40),   # Ravnsborggade
    (55.68710, 12.56430, 36, True, 40),   # Dronning Louises Bro
    (55.68520, 12.56820, 30, True, 40),   # Frederiksborggade
    (55.68360, 12.57160, 30, True, 40),   # Nørreport, turning onto Øster Voldgade
    (55.68580, 12.57550, 42, False, 40),  # Øster Voldgade, between the gardens
    (55.68850, 12.57950, 42, True, 40),   # Sølvtorvet
    (55.69050, 12.58300, 40, True, 40),
    (55.69270, 12.58680, 30, True, 40),   # Østerport
    (55.69500, 12.58520, 36, True, 40),   # Oslo Plads
    (55.69800, 12.58200, 38, False, 40),  # Østerbrogade
]

_NORREPORT_TO_AMAGER = [
    (55.68710, 12.56430, 30, False, 40),  # Dronning Louises Bro, heading south-east
    (55.68520, 12.56820, 30, True, 40),   # Frederiksborggade
    (55.68340, 12.57220, 32, True, 40),   # Nørreport, turning east onto Gothersgade
    (55.68220, 12.57650, 34, True, 40),   # Gothersgade by Rosenborg
    (55.68120, 12.58050, 34, True, 40),   # Store Kongensgade
    (55.68010, 12.58480, 28, True, 40),   # Kongens Nytorv
    (55.67780, 12.58430, 34, True, 40),   # Holmens Kanal
    (55.67570, 12.58550, 34, True, 40),   # Børsgade
    (55.67440, 12.58860, 40, False, 40),  # Knippelsbro
    (55.67300, 12.59200, 32, True, 40),   # Christianshavns Torv
    (55.67140, 12.59520, 38, True, 40),   # Torvegade
    (55.66960, 12.59840, 34, True, 40),   # Christmas Møllers Plads
    (55.66680, 12.60030, 40, True, 40),   # Amagerbrogade
    (55.66380, 12.60240, 40, True, 40),
    (55.66080, 12.60430, 40, True, 40),
    (55.65780, 12.60620, 40, False, 40),
]

_ENGHAVE_TO_DEPOT = [
    (55.66900, 12.54520, 30, True, 40),   # Enghavevej, heading south
    (55.66700, 12.54600, 28, True, 40),   # Enghave Plads
    (55.66450, 12.54680, 34, True, 40),
    (55.66280, 12.54850, 30, True, 40),
    (55.66200, 12.55100, 18, False, 30),  # the depot gate
    (55.66180, 12.55280, 0, False, 30),   # the depot
]


class _Route:
    """A drive along a polyline of streets; s is the distance along it in metres."""

    def __init__(self, pts: Sequence[Tuple[float, float, float, bool, int]]):
        self.pts = list(pts)
        self.cum = [0.0]
        self.brg: List[float] = []
        for (la0, lo0, *_), (la1, lo1, *_) in zip(self.pts, self.pts[1:]):
            dy = (la1 - la0) * M_PER_DEG_LAT
            dx = (lo1 - lo0) * M_PER_DEG_LON
            self.cum.append(self.cum[-1] + math.hypot(dx, dy))
            self.brg.append(math.degrees(math.atan2(dx, dy)) % 360.0)
        self.length = self.cum[-1]
        self.signals = [self.cum[i] for i, p in enumerate(self.pts) if p[3] and i > 0]

    def reversed(self) -> "_Route":
        rev = list(reversed(self.pts))
        # A segment's target and limit belong to its start, so shift them by one.
        out = [(la, lo, rev[i + 1][2] if i + 1 < len(rev) else 0, sig,
                rev[i + 1][4] if i + 1 < len(rev) else lim)
               for i, (la, lo, _, sig, lim) in enumerate(rev)]
        return _Route(out)

    def _seg(self, s: float) -> int:
        for i in range(len(self.cum) - 1):
            if s < self.cum[i + 1]:
                return i
        return len(self.cum) - 2

    def target_kmh(self, s: float) -> float:
        return float(self.pts[self._seg(s)][2])

    def limit_kmh(self, s: float) -> int:
        return self.pts[self._seg(min(max(s, 0.0), self.length))][4]

    def where(self, s: float) -> Tuple[float, float, float]:
        """Latitude, longitude and bearing at s."""
        s = min(max(s, 0.0), self.length)
        i = self._seg(s)
        seg = self.cum[i + 1] - self.cum[i]
        f = (s - self.cum[i]) / seg if seg > 0 else 0.0
        (la0, lo0, *_), (la1, lo1, *_) = self.pts[i], self.pts[i + 1]
        return la0 + (la1 - la0) * f, lo0 + (lo1 - lo0) * f, self.brg[i]


# --------------------------------------------------------------------------- helpers


def _r(x: float, nd: int) -> float:
    """Round for publishing, without a negative zero."""
    v = round(x, nd)
    return v if v != 0 else 0.0


class _OU:
    """Mean-reverting noise (Ornstein-Uhlenbeck) with time constant `tau` and standard
    deviation `sigma`, stepped `dt` seconds at a time."""

    def __init__(self, rng: random.Random, tau: float, sigma: float, dt: float = 1.0):
        self.rng, self.tau, self.dt = rng, tau, dt
        self.x = rng.gauss(0.0, sigma)
        self.k = sigma * math.sqrt(2.0 * dt / tau)

    def step(self) -> float:
        self.x += -self.x * self.dt / self.tau + self.k * self.rng.gauss(0.0, 1.0)
        return self.x


class _Streams:
    """A vehicle's random streams, each seeded up front from the vehicle's seed: `drive`
    (signal timings, traffic), `proc` (engine and charger noise, second by second) and
    `sensor` (reporting phase and measurement noise, record by record). Each is drawn in
    time order, so a longer run only appends draws and never shifts earlier ones."""

    def __init__(self, seed: int):
        g = random.Random(seed)
        self.drive = random.Random(g.getrandbits(64))
        self.proc = random.Random(g.getrandbits(64))
        self.sensor = random.Random(g.getrandbits(64))


def _vin(first8: str, year: str, plant: str, serial: str) -> str:
    """A VIN with its 49 CFR 565 check digit in position 9."""
    values = dict(zip("ABCDEFGH", range(1, 9)))
    values.update(zip("JKLMN", range(1, 6)))
    values.update({"P": 7, "R": 9})
    values.update(zip("STUVWXYZ", range(2, 10)))
    values.update({str(d): d for d in range(10)})
    weights = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
    body = first8 + "0" + year + plant + serial
    check = sum(values[c] * w for c, w in zip(body, weights)) % 11
    return first8 + ("X" if check == 10 else str(check)) + year + plant + serial


def _at_list(phase: float, period: float, start: float, end: float) -> List[float]:
    """Record times phase + k * period inside [start, end)."""
    out, k = [], 0
    while phase + k * period < end:
        at = phase + k * period
        if at >= start:
            out.append(round(at, 3))
        k += 1
    return out


def _lerp(xs: List[float], t: float) -> float:
    i = max(0, min(len(xs) - 2, int(math.floor(t))))
    f = min(1.0, max(0.0, t - i))
    return xs[i] * (1 - f) + xs[i + 1] * f


# --------------------------------------------------------------------------- driving

AMBER_MAX = 2.6  # m/s2: a driver who would have to brake harder for a red goes on amber
RUSH = 0.8  # the evening rush: everyone below the speed they would hold in free traffic


class _Signals:
    """Fixed-time traffic signals: an 80-100 s cycle, green for a share of it."""

    def __init__(self, rng: random.Random, positions: List[float], green: Tuple[float, float]):
        self.pos = positions
        self.timing = []
        for _ in positions:
            cycle = rng.choice((80.0, 90.0, 100.0))
            self.timing.append((cycle, cycle * rng.uniform(*green), rng.uniform(0.0, cycle)))

    def red(self, i: int, t: float) -> bool:
        cycle, green, offset = self.timing[i]
        return (t + offset) % cycle >= green


class _Trace:
    """Per-second kinematics: s (m along the route), v (m/s), a[i], the mean acceleration
    over the second that ends at i, and parked[i], whether the selector is in P at i.
    kerb_at: the second a vehicle told to pull in stood at the kerb (None otherwise)."""

    def __init__(self, route: Optional[_Route], s: List[float], v: List[float], a: List[float],
                 parked: List[bool], kerb_at: Optional[int] = None):
        self.route, self.s, self.v, self.a, self.parked = route, s, v, a, parked
        self.kerb_at = kerb_at

    def speed_kmh(self, t: float) -> float:
        return _lerp(self.v, t) * 3.6

    def in_park(self, t: float) -> bool:
        return self.parked[max(0, min(len(self.parked) - 1, int(math.floor(t))))]

    def window_ax(self, t: float) -> Tuple[float, float]:
        """Of the five 1-s means ending at or before t, the one of largest magnitude, and
        the speed (m/s) at the start of its second."""
        i = int(math.floor(t))
        best = None
        for j in range(max(1, i - 4), i + 1):
            if best is None or abs(self.a[j]) > abs(self.a[best]):
                best = j
        if best is None:
            return 0.0, self.v[0]
        return self.a[best], self.v[best - 1]


def _drive(rng: random.Random, route: _Route, n: int, *, s0: float = 0.0, v0: float = 0.0,
           depart: int = 0, a_max: float = 1.6, b_comf: float = 1.8, scale: float = 1.0,
           green: Tuple[float, float] = (0.45, 0.6),
           harsh: Optional[Tuple[int, float, int]] = None,
           fast: Optional[Tuple[float, float, float, float]] = None,
           pull_in: Optional[Tuple[int, float]] = None) -> _Trace:
    """Drive `route` for n seconds. `rng` is drawn the same way whatever the options, so two
    drives that differ only in `pull_in` are the same second by second until it.

    harsh: (earliest second, peak deceleration m/s2, seconds to wait): the first time the
      vehicle is at 25 km/h or more after the earliest second, someone steps out: the
      driver stops within about 2 s, the second of them at the peak, waits, and drives on.
    fast: (from m, to m, km/h, acceleration): a stretch the driver takes far too fast,
      on green.
    pull_in: (second, until): from that second the driver heads for the kerb at least 30 m
      on, and stands there until then (a job; float('inf') to stay).
    """
    sig = _Signals(rng, route.signals, green)
    traffic = _OU(rng, 20.0, 0.09)
    stops: List[Tuple[float, float]] = []  # (position m, until s): the kerb of a pull-in
    s, v = s0, v0
    S, V, A = [s], [v], [0.0]
    harsh_left: List[float] = []
    harsh_done = harsh is None
    hold_until = -1
    kerb: Optional[float] = None
    for t in range(n):
        x = traffic.step()
        if t < depart or t < hold_until:
            a = -v
        elif harsh_left:
            a = harsh_left.pop(0)
            if not harsh_left:
                hold_until = t + 1 + harsh[2]
        elif not harsh_done and t >= harsh[0] and v >= 7.0:
            harsh_done = True
            peak = harsh[1]
            first = -max(1.0, min(3.6, v - peak))
            harsh_left = [-min(peak, v + first), -max(0.0, v + first - peak)]
            a = first
        else:
            if pull_in is not None and kerb is None and t >= pull_in[0]:
                kerb = s + max(30.0, v * v / 2.0)
                stops.append((kerb, pull_in[1]))
            on_fast = fast is not None and fast[0] <= s < fast[1]
            target = route.target_kmh(s) / 3.6 * RUSH * scale * (1.0 + max(-0.35, min(0.08, x)))
            target = min(target, 49.8 / 3.6)
            accel = a_max
            if on_fast:
                target, accel = fast[2] / 3.6, fast[3]
            stopping = False
            d_stop = float("inf")
            if not on_fast:
                for i, p in enumerate(sig.pos):
                    if p <= s or p - s > 120.0:
                        continue
                    if sig.red(i, t):
                        d = p - 3.0 - s  # the stop line
                        if d > 0 and v * v / (2.0 * d) > AMBER_MAX:
                            continue  # too late to stop: over on amber
                        d_stop = d
                        break
            for p, until in stops:
                if s < p + 0.5 and t < until:
                    d_stop = min(d_stop, p - s)
            d_stop = min(d_stop, route.length - 1.0 - s)
            if d_stop < float("inf"):
                # The highest speed at the END of this second from which the driver can
                # still stop at b_comf: v1^2 = 2 b (d - (v + v1) / 2).
                b = b_comf * max(0.75, min(1.3, 1.0 + 2.0 * x))  # this driver, this time
                room = d_stop - v / 2.0
                v_allow = 0.0 if room <= 0 else (-b + math.sqrt(b * b + 8.0 * b * room)) / 2.0
                if v_allow < target:
                    target, stopping = v_allow, True
            if v < target:
                a = min(target - v, accel * (1.0 - (v / max(target, 0.1)) ** 4))
            else:
                a = max(target - v, -AMBER_MAX if stopping else -1.2)
        v1 = max(0.0, v + a)
        a = v1 - v
        s += (v + v1) / 2.0
        v = v1
        S.append(s)
        V.append(v)
        A.append(a)
    # In P: before setting off, and standing at a planned stop (a job, the depot, the kerb,
    # the end of the route). A stop at a red light or behind a pedestrian is in D.
    parked = []
    kerb_at = None
    for t in range(n + 1):
        at_kerb = (kerb is not None and pull_in[0] <= t < pull_in[1] and V[t] == 0.0
                   and abs(S[t] - kerb) < 5.0)
        if at_kerb and kerb_at is None:
            kerb_at = t
        parked.append(t < depart or at_kerb or (V[t] == 0.0 and S[t] >= route.length - 5.0))
    return _Trace(route, S, V, A, parked, kerb_at)


def _parked(n: int) -> _Trace:
    return _Trace(None, [0.0] * (n + 1), [0.0] * (n + 1), [0.0] * (n + 1), [True] * (n + 1))


# --------------------------------------------------------------------------- EVs


class _EV:
    """A battery-electric vehicle: its kinematics, battery and TCU."""

    def __init__(self, st: _Streams, vin: str, usable_kwh: float, mass: float, cda: float,
                 aux_kw: float):
        self.st = st
        self.vin = vin
        self.usable_kwh, self.mass, self.cda, self.aux_kw = usable_kwh, mass, cda, aux_kw
        sens = st.sensor
        self.phase = round(sens.uniform(0.3, 4.7), 2)  # the TCU's 5-s reporting phase
        self.gps = (_OU(sens, 30.0, 1.6, dt=5.0), _OU(sens, 30.0, 1.6, dt=5.0))  # m east, north
        self.hdg: Optional[float] = None
        self.soc: List[float] = []
        self.pack: List[float] = []
        self.odo: List[float] = []

    def energy(self, tr: _Trace, soc0: float, pack0: float, odo0: float, on) -> None:
        """Battery state second by second while driving; `on(t)` is ignition on."""
        soc, pack, odo = soc0, pack0, odo0
        self.soc, self.pack, self.odo = [soc], [pack], [odo]
        for t in range(len(tr.v) - 1):
            v0, v1 = tr.v[t], tr.v[t + 1]
            vm, a = (v0 + v1) / 2.0, v1 - v0
            p_wheel = self.mass * a * vm + self.mass * 9.81 * 0.011 * vm + 0.5 * 1.27 * self.cda * vm ** 3
            p_batt = p_wheel / 0.90 if p_wheel >= 0 else max(p_wheel * 0.70, -60_000.0)
            if on(t):
                p_batt += self.aux_kw * 1000.0
            soc -= p_batt / 3600.0 / (self.usable_kwh * 1000.0) * 100.0
            pack += 0.00010 * abs(p_batt) / 1000.0 - 0.00008 * (pack - 12.0)
            odo += vm / 1000.0
            self.soc.append(soc)
            self.pack.append(pack)
            self.odo.append(odo)

    def record(self, t0: float, at: float, tr: _Trace, chg: str = "off", chg_kw: float = 0.0) -> dict:
        sens = self.st.sensor
        if tr.route is not None:
            s = _lerp(tr.s, at)
            lat, lon, brg = tr.route.where(s)
            lim: Optional[int] = tr.route.limit_kmh(s)
        else:
            lat, lon, brg = DC_CHARGER[0], DC_CHARGER[1], 214.0
            lim = None  # a car park: no posted limit in the map
        east, north = self.gps[0].step(), self.gps[1].step()
        lat += north / M_PER_DEG_LAT
        lon += east / M_PER_DEG_LON
        spd = tr.speed_kmh(at)
        if self.hdg is None or spd >= 1.5:
            self.hdg = (brg + sens.gauss(0.0, 1.5)) % 360.0
        spd_out = 0.0 if spd < 0.05 else max(0.0, spd + sens.gauss(0.0, 0.15))
        ax, v_ax = tr.window_ax(at)
        ax += sens.gauss(0.0, 0.06) + (sens.gauss(0.0, 0.08) if spd > 1 else 0.0)
        ax_spd = 0.0 if v_ax * 3.6 < 0.05 else max(0.0, v_ax * 3.6 + sens.gauss(0.0, 0.15))
        return {
            "ts": ts_ms(t0, at),
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "hdg": int(round(self.hdg)) % 360,
            "spd": _r(spd_out, 1),
            "lim": lim,
            "gear": "P" if tr.in_park(at) else "D",
            "ax": _r(ax, 2),
            "ax_spd": _r(ax_spd, 1),
            "soc": _r(_lerp(self.soc, at), 1),
            "bat_t": _r(_lerp(self.pack, at) + sens.gauss(0.0, 0.05), 1),
            "odo": _r(_lerp(self.odo, at), 1),
            "chg": chg,
            "chg_kw": _r(chg_kw, 1),
        }

    def topic(self, stream: str) -> str:
        return f"vehicle/{self.vin}/{stream}"

    def live(self, t0: float, at: float, tr: _Trace, **kw) -> Event:
        return Event(at=at, client=self.vin, topic=self.topic("telemetry"), qos=1,
                     payload=jbytes(self.record(t0, at, tr, **kw)))


def _soc_to_cross(ev: _EV, tr: _Trace, at: float, level: float, on) -> float:
    """The starting SoC that makes the battery fall through `level` at `at` s."""
    ev.energy(tr, 100.0, 15.0, 0.0, on)
    used = 100.0 - _lerp(ev.soc, at)
    return level + used


# EV-A: an e-van back to the depot, almost empty.
VIN_A = _vin("XDKVE4T2", "R", "H", "204117")
LOW_SOC_AT = 410.0  # the BMS's SoC crosses 15.0 here; the 1-decimal reading shows 14.9 a little later
# EV-B: DC fast charging; the battery chiller's pump seizes.
VIN_B = _vin("XDKCE7K3", "S", "H", "118402")
CHILLER_FAILS_AT = 30
BMS_DERATE_C = 48.5
# EV-C: an e-van; harsh braking, then a job (ignition off and on).
VIN_C = _vin("XDKVE6T7", "S", "H", "090233")
HARSH_EARLIEST, HARSH_PEAK, HARSH_HOLD = 170, 5.4, 3
C_JOB_AT, C_ON_AT = 300, 480.0  # pulls in at the kerb for a job; ignition on again
# EV-D: ignition on at a customer; speeds on Øster Voldgade.
VIN_D = _vin("XDKCE5K3", "P", "H", "311806")
D_ON_AT, D_DEPARTS = 40.0, 50
# EV-E: loses its connection, buffers, uploads a trip log.
VIN_E = _vin("XDKCE5K3", "R", "H", "287145")
E_LOST_AT, E_BACK_AT = 141.0, 305.0
# EV-F: back to the depot, plugged in to wait for off-peak, ignition off.
VIN_F = _vin("XDKCE5K3", "N", "H", "165530")
F_PLUGS_IN, F_SWITCHES_OFF = 20.0, 34.0  # seconds after it stops in its bay


def _ev_a(st: _Streams, t0: float, duration: float, n: int) -> List[Event]:
    ev = _EV(st, VIN_A, 45.0, 2050.0, 0.85, 2.2)
    route = _Route(_OSTERPORT_TO_DEPOT)
    tr = _drive(st.drive, route, n, s0=40.0, v0=6.0, a_max=1.3, b_comf=1.7, scale=0.9)
    on = lambda t: True  # noqa: E731
    soc0 = _soc_to_cross(ev, tr, LOW_SOC_AT, 15.0, on)
    ev.energy(tr, soc0, 17.8, 38_412.6, on)
    return [ev.live(t0, at, tr) for at in _at_list(ev.phase, 5.0, 0.0, duration)]


def _ev_b(st: _Streams, t0: float, duration: float, n: int) -> List[Event]:
    """DC charging: the charger's power follows the car's charging curve and its BMS's
    temperature derating; the pack heats as I^2 R (an older pack, 0.105 ohm) and is cooled
    by its chiller until the chiller's pump fails; it also loses 55 W/K to the 7 degC air."""
    ev = _EV(st, VIN_B, 77.0, 2100.0, 0.62, 0.6)
    tr = _parked(n)
    soc, temp = 31.2, 42.2
    soc_l, temp_l, kw_l = [soc], [temp], []
    for t in range(n):
        curve = 135.0 if soc < 45 else max(40.0, 135.0 - (soc - 45.0) * 2.6)
        derate = 1.0 if temp < BMS_DERATE_C else max(0.25, 1.0 - (temp - BMS_DERATE_C) * 0.12)
        kw = curve * derate + st.proc.gauss(0.0, 0.4)
        volts = 352.0 + soc * 0.95
        amps = kw * 1000.0 / volts
        heat = amps * amps * 0.105
        cool = min(14_000.0, 950.0 * max(0.0, temp - 27.0)) if t < CHILLER_FAILS_AT else 0.0
        temp += (heat - cool - 55.0 * (temp - 7.0)) / 420_000.0
        soc += kw * 0.96 / 3600.0 / 77.0 * 100.0
        soc_l.append(soc)
        temp_l.append(temp)
        kw_l.append(kw)
    kw_l.append(kw_l[-1])
    ev.soc, ev.pack, ev.odo = soc_l, temp_l, [22_874.3] * (n + 1)
    return [ev.live(t0, at, tr, chg="dc", chg_kw=_lerp(kw_l, at))
            for at in _at_list(ev.phase, 5.0, 0.0, duration)]


def _ev_c(st: _Streams, t0: float, duration: float, n: int) -> List[Event]:
    ev = _EV(st, VIN_C, 68.0, 2900.0, 1.05, 2.4)
    route = _Route(_VESTERBRO_TO_AMAGER)
    tr = _drive(st.drive, route, n, s0=60.0, v0=7.0, a_max=1.2, b_comf=1.6, scale=0.92,
                pull_in=(C_JOB_AT, C_ON_AT + 8.0), harsh=(HARSH_EARLIEST, HARSH_PEAK, HARSH_HOLD))
    arrived = tr.kerb_at if tr.kerb_at is not None else n
    off = float(arrived + 12)  # the driver switches off and gets out
    on = lambda t: not (off <= t < C_ON_AT)  # noqa: E731
    ev.energy(tr, 63.4, 15.1, 51_207.9, on)
    out = []
    for at in _at_list(ev.phase, 5.0, 0.0, duration):
        if off <= at < C_ON_AT:
            continue
        out.append(ev.live(t0, at, tr))
    if off < min(duration, C_ON_AT):
        out.append(Event(at=off, client=VIN_C, kind="disconnect"))
        if C_ON_AT < duration:
            out.append(Event(at=C_ON_AT, client=VIN_C, kind="connect"))
    return out


def _ev_d(st: _Streams, t0: float, duration: float, n: int) -> List[Event]:
    ev = _EV(st, VIN_D, 58.0, 1950.0, 0.64, 1.5)
    route = _Route(_NORREBRO_TO_OSTERPORT)
    # The stretch of Øster Voldgade between the Botanical Garden and Kongens Have.
    fast = (route.cum[6] + 120.0, route.cum[6] + 330.0, 68.0, 2.4)
    tr = _drive(st.drive, route, n, depart=D_DEPARTS, a_max=2.0, b_comf=2.0, fast=fast)
    on = lambda t: t >= D_ON_AT  # noqa: E731
    ev.energy(tr, 71.9, 13.4, 9_381.2, on)
    out = [Event(at=D_ON_AT, client=VIN_D, kind="connect")]
    out += [ev.live(t0, at, tr) for at in _at_list(ev.phase, 5.0, D_ON_AT, duration)]
    return out


def _ev_e(st: _Streams, t0: float, duration: float, n: int) -> List[Event]:
    ev = _EV(st, VIN_E, 58.0, 1950.0, 0.64, 1.5)
    route = _Route(_NORREPORT_TO_AMAGER)
    tr = _drive(st.drive, route, n, s0=10.0, v0=3.0, a_max=1.8, b_comf=1.9)
    ev.energy(tr, 48.6, 16.2, 27_655.0, lambda t: True)
    out: List[Event] = []
    buffered: List[dict] = []
    up = E_BACK_AT + 0.6  # the backlog goes first, as soon as the session is up
    for at in _at_list(ev.phase, 5.0, 0.0, duration):
        if E_LOST_AT <= at < up:
            buffered.append(ev.record(t0, at, tr))  # logged to flash, not sent
        else:
            out.append(ev.live(t0, at, tr))
    out.append(Event(at=E_LOST_AT, client=VIN_E, kind="drop"))
    if buffered and up < duration:
        out.append(Event(at=E_BACK_AT, client=VIN_E, kind="connect"))
        out.append(Event(at=up, client=VIN_E, topic=ev.topic("triplog"), qos=1, payload=jbytes({
            "ts": ts_ms(t0, up),
            "reason": "backlog",
            "from": buffered[0]["ts"],
            "to": buffered[-1]["ts"],
            "n": len(buffered),
            "records": buffered,
        })))
    return out


def _ev_f(st: _Streams, t0: float, duration: float, n: int) -> List[Event]:
    ev = _EV(st, VIN_F, 58.0, 1950.0, 0.64, 1.5)
    route = _Route(_ENGHAVE_TO_DEPOT)
    tr = _drive(st.drive, route, n, s0=15.0, v0=5.0, a_max=1.7, b_comf=1.8, scale=0.95)
    arrived = next((t for t in range(n) if tr.s[t] > route.length - 3.0 and tr.v[t] == 0.0), n)
    plugged, off = arrived + F_PLUGS_IN, float(arrived + F_SWITCHES_OFF)
    on = lambda t: t < off  # noqa: E731
    ev.energy(tr, 38.7, 16.9, 14_220.4, on)
    out = []
    for at in _at_list(ev.phase, 5.0, 0.0, min(duration, off)):
        out.append(ev.live(t0, at, tr, chg="wait" if at >= plugged else "off"))
    if off < duration:
        out.append(Event(at=off, client=VIN_F, kind="disconnect"))
    return out


# --------------------------------------------------------------------------- diesel vans

ECU_ENGINE = 0x7E8
# rpm per km/h in each gear of a 6-speed 2.0 l diesel van
GEARS = (105.0, 57.0, 36.5, 27.0, 21.5, 18.0)
TANK_L = 80.0
LAMP_C = 115.0  # the dashboard's coolant warning lamp; the ECU stores P0217 with it

VIN_V1 = _vin("XDKVD2T5", "N", "H", "140928")
V1_DTC_AT, V1_DTC = 215.0, "P0301"
VIN_V2 = _vin("XDKVD2T5", "P", "H", "151374")
FAN_FAILS_AT = 150
VIN_V3 = _vin("XDKVD2T5", "R", "H", "163019")
V3_DTC_AT, V3_DTC = 330.0, "P0128"


def _dtc_bytes(code: str) -> bytes:
    """'P0301' as its two bytes (SAE J2012): system, first digit, then three hex digits."""
    system = "PCBU".index(code[0])
    first = int(code[1])
    rest = int(code[2:], 16)
    return bytes([(system << 6) | (first << 4) | (rest >> 8), rest & 0xFF])


def _header(t0: float, at: float) -> bytes:
    return struct.pack(">IH", ts_ms(t0, at) // 1000, ECU_ENGINE)


def _engine(rng: random.Random, tr: _Trace, n: int, coolant: str, coolant0: float,
            fuel_pct: float, off_at: Optional[float], rough_from: Optional[int]):
    """The engine second by second: rpm (from the speed and the gear), coolant, fuel.

    coolant: 'normal' (the thermostat holds 88-93 degC), 'fan_fails' (from FAN_FAILS_AT the
    radiator has only the air the van's own speed pushes through it) or 'stuck_open'
    (the thermostat never closes: the engine runs cold in 6 degC air).
    """
    gear, temp, fuel = 1, coolant0, fuel_pct / 100.0 * TANK_L
    rpm_l, temp_l, fuel_l = [], [], []
    for t in range(n + 1):
        kmh = tr.v[t] * 3.6
        a = tr.a[t]
        running = off_at is None or t < off_at
        if kmh < 6.0:
            gear = 1
            rpm = 790.0 + (300.0 if a > 0.3 else 0.0)
        else:
            while gear < 6 and kmh * GEARS[gear - 1] > 2100.0:
                gear += 1
            while gear > 1 and kmh * GEARS[gear - 1] < 1150.0:
                gear -= 1
            rpm = max(790.0, kmh * GEARS[gear - 1])
        rough = 45.0 if rough_from is not None and t >= rough_from else 12.0
        rpm += rng.gauss(0.0, rough)
        load = max(0.0, min(1.0, a / 1.2 + kmh / 60.0))
        if coolant == "stuck_open":
            temp += (72.5 - 0.17 * kmh + 4.0 * load - temp) / 90.0
        elif coolant == "fan_fails" and t >= FAN_FAILS_AT:
            if running:
                temp += max(0.115 - 0.0034 * kmh + 0.012 * load, (89.5 - temp) / 60.0)
        else:
            temp += (88.6 + 3.5 * load - temp) / 60.0
        temp += rng.gauss(0.0, 0.04)
        if running:
            fuel -= 0.00022 + 0.000085 * tr.v[t] + 0.0004 * load * (tr.v[t] > 0)
        rpm_l.append(rpm if running else 0.0)
        temp_l.append(temp)
        fuel_l.append(fuel)
    return rpm_l, temp_l, fuel_l


def _van(st: _Streams, vin: str, route: _Route, t0: float, duration: float, n: int, *,
         s0: float, v0: float, scale: float, green: Tuple[float, float], fuel_pct: float,
         coolant: str, coolant0: float, dtc: Optional[Tuple[float, str]] = None,
         rough_from: Optional[int] = None) -> List[Event]:
    """One diesel van and its OBD-II dongle.

    With coolant 'fan_fails', the driver pulls over when the coolant warning lamp lights
    (LAMP_C), and switches off 9 s after standing at the kerb; the ECU stores P0217 with
    the lamp, and the dongle sends it 2 s later. The drive is computed twice: once to find
    when the lamp lights, and again, from the same random state, with the pull-over.
    """
    phase = round(st.sensor.uniform(0.5, 9.5), 2)
    drive_kw = dict(s0=s0, v0=v0, a_max=1.1, b_comf=1.6, scale=scale, green=green)
    saved = (st.drive.getstate(), st.proc.getstate())
    tr = _drive(st.drive, route, n, **drive_kw)
    rpm_l, temp_l, fuel_l = _engine(st.proc, tr, n, coolant, coolant0, fuel_pct, None, rough_from)
    off_at = None
    dtcs = [dtc] if dtc is not None else []
    if coolant == "fan_fails":
        lamp = next((t for t in range(FAN_FAILS_AT, n + 1) if temp_l[t] >= LAMP_C), None)
        if lamp is not None:
            st.drive.setstate(saved[0])
            st.proc.setstate(saved[1])
            tr = _drive(st.drive, route, n, pull_in=(lamp, float("inf")), **drive_kw)
            if tr.kerb_at is not None:
                off_at = float(tr.kerb_at + 9)  # the driver switches off
            rpm_l, temp_l, fuel_l = _engine(st.proc, tr, n, coolant, coolant0, fuel_pct, off_at,
                                            rough_from)
            dtcs.append((lamp + 2.0, "P0217"))

    out: List[Event] = []
    end = duration if off_at is None else min(duration, off_at)
    for at in _at_list(phase, 10.0, 0.0, end):
        i = int(math.floor(at))  # one instant for every PID, as the ECU answers
        rpm_raw = max(0, min(65535, int(round(rpm_l[i] * 4))))
        kmh = max(0, min(255, int(round(tr.v[i] * 3.6))))
        cool = max(0, min(255, int(round(temp_l[i])) + 40))
        level = fuel_l[i] / TANK_L * 100.0 + st.sensor.gauss(0.0, 0.25)  # the sender sloshes
        fuel_raw = max(0, min(255, int(round(level * 255.0 / 100.0))))
        body = bytes([0x41, 0x0C, rpm_raw >> 8, rpm_raw & 0xFF, 0x0D, kmh, 0x05, cool, 0x2F, fuel_raw])
        out.append(Event(at=at, client=vin, topic=f"vehicle/{vin}/obd", payload=_header(t0, at) + body))
    for at, code in dtcs:
        if at < end:
            out.append(Event(at=at, client=vin, topic=f"vehicle/{vin}/dtc", qos=1,
                             payload=_header(t0, at) + bytes([0x43, 0x01]) + _dtc_bytes(code)))
    if off_at is not None and off_at < duration:
        out.append(Event(at=off_at, client=vin, kind="disconnect"))
    return out


# --------------------------------------------------------------------------- the fleet

_SCENARIOS = ("A", "B", "C", "D", "E", "F", "V1", "V2", "V3")
SCRIPTED_S = 600  # the scenario is written for 10 minutes; after that vehicles park at their routes' ends


def events(rng: random.Random, t0: float, duration: float) -> Iterable[Event]:
    """The fleet's messages for `duration` seconds from `t0` (Unix seconds)."""
    seeds: Dict[str, int] = {name: rng.getrandbits(64) for name in _SCENARIOS}
    st = {name: _Streams(seed) for name, seed in seeds.items()}
    # The physics always covers the scripted 10 minutes (and more for a longer run); the
    # streams make the first N seconds the same whatever the duration.
    n = max(SCRIPTED_S, int(math.ceil(duration))) + 6
    out: List[Event] = []
    out += _ev_a(st["A"], t0, duration, n)
    out += _ev_b(st["B"], t0, duration, n)
    out += _ev_c(st["C"], t0, duration, n)
    out += _ev_d(st["D"], t0, duration, n)
    out += _ev_e(st["E"], t0, duration, n)
    out += _ev_f(st["F"], t0, duration, n)
    out += _van(st["V1"], VIN_V1, _Route(_NORREPORT_TO_AMAGER).reversed(), t0, duration, n,
                s0=250.0, v0=8.0, scale=0.95, green=(0.45, 0.6), fuel_pct=54.3,
                coolant="normal", coolant0=90.1, dtc=(V1_DTC_AT, V1_DTC), rough_from=170)
    out += _van(st["V2"], VIN_V2, _Route(_OSTERPORT_TO_DEPOT), t0, duration, n,
                s0=1900.0, v0=2.0, scale=0.55, green=(0.30, 0.38), fuel_pct=31.6,
                coolant="fan_fails", coolant0=89.8)
    out += _van(st["V3"], VIN_V3, _Route(_VESTERBRO_TO_AMAGER).reversed(), t0, duration, n,
                s0=1200.0, v0=9.0, scale=1.0, green=(0.45, 0.6), fuel_pct=77.2,
                coolant="stuck_open", coolant0=67.0, dtc=(V3_DTC_AT, V3_DTC))
    out.sort(key=lambda e: e.at)  # stable: a connect stays before a publish at the same time
    return out
