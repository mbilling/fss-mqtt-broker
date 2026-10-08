"""Homes: a dozen Danish households, and the devices in them that publish over MQTT.

The setting is Zealand, Denmark (price area DK2): a 50 Hz, 3 x 230/400 V supply in every
home, metric units, local time CET (UTC+1), or CEST (UTC+2) from the last Sunday of March
to the last Sunday of October. Behaviour follows the real clock: the sun sets on time,
outdoor temperatures follow the season and the hour, and the batteries, the smart EV
charger and the thermostat schedules act on local time (the DSO's peak tariff runs
17:00-21:00), and the heat pumps stop space heating above a 15 degC daily mean outdoors.
The injected faults run on the simulation's own clock (seconds after --start), so a 600 s
run in the heating season contains every one of them; what each needs from the season or
the hour is said below.

Devices, on home/<home-id>/<device>, each its own MQTT client "<home-id>-<device>":

p1          A reader on the smart meter's P1 customer port (DSMR 5.0.2, the Dutch
            smart-meter standard; Nordic HAN readers for Kamstrup/Aidon meters translate
            their DLMS/COSEM frames into the same OBIS lines). Every 10 s it publishes the
            meter's raw telegram as ASCII text, CRLF line ends, QoS 0: the "/XMX5LG..."
            identification line, a blank line, OBIS lines such as "0-0:1.0.0(260324165503W)"
            (local time YYMMDDhhmmss, W = winter / S = summer time),
            "0-0:96.1.1(<equipment id, hex-encoded ASCII>)",
            "1-0:1.8.1(004127.338*kWh)" / 1.8.2 (delivered to the home, tariff 1 / 2),
            2.8.1 / 2.8.2 (returned to the grid), "0-0:96.14.0(0002)" (active tariff),
            "1-0:1.7.0(01.193*kW)" / 2.7.0 (actual power delivered / returned), power-failure
            counters and log, voltage sag / swell counters per phase, per-phase voltage
            (32.7.0 / 52.7.0 / 72.7.0, 0.1 V), current (31.7.0 / 51.7.0 / 71.7.0, whole A) and
            power delivered / returned (21.7.0 ... 62.7.0, W resolution in kW), then "!" and
            the CRC16 (CRC-16/ARC over "/" ... "!") in upper-case hex. Tariff 2 (normal) runs
            on weekdays 07:00-23:00, tariff 1 (low) otherwise: the meter's own two-register
            scheme, not the Danish three-period grid tariff. The meter integrates the
            house's net power (all loads, minus PV, plus or minus the battery) into its
            registers, so the telegrams agree with every other device in the home.
thermostat  A room thermostat, JSON every 60 s, QoS 0:
            {"ts": ms, "temp_c": 21.3, "setpoint_c": 21.5, "setpoint_since": ms,
             "humidity_pct": 41, "mode": "heat" | "away", "heating": true}  (heating = it is
            calling for heat; temperatures in 0.1 degC, relative humidity in whole %).
            setpoint_since is when the current setpoint took effect, Unix ms: the start of
            the schedule slot, or the owner's last change (18:00 on 1 October, when the
            heating season began, for the homes without a schedule), as thermostat cloud
            APIs report it (Netatmo's therm_setpoint_start_time, for one).
heatpump    An air-to-water monobloc heat pump's controller (as a Modbus or vendor-cloud
            bridge would publish it), JSON every 30 s, QoS 0:
            {"ts": ms, "mode": "heat" | "dhw" | "standby", "compressor": true,
             "compressor_hz": 46, "elec_w": 1012, "heat_w": 3870, "flow_c": 35.2,
             "return_c": 31.8, "flow_lpm": 16.3, "outdoor_c": 5.4, "defrost": false,
             "fault": "", "fault_since": null}
            fault is the active fault code and text ("" without one), fault_since when it
            was raised (Unix ms, from the controller's alarm log; null without a fault).
            elec_w is the electrical input (compressor, fan, circulation pump, controls),
            heat_w the heat delivered to the water (flow x 4186 J/(kg K) x (flow - return)),
            so heat_w / elec_w is the COP. Flow temperature follows a weather-compensation
            curve; the COP follows the temperature lift at about half the Carnot COP, which
            gives 3.0-3.7 at 4-6 degC outdoor for these homes, radiators to underfloor
            heating (EN 14511 ratings at A2/W35 are typically 3.5-4.2). In a defrost cycle
            the unit runs reversed and flags "defrost" from the moment it reverses: heat_w
            goes negative for a few minutes. The 3 kW immersion heater in the hot-water tank
            (legionella cycle, backup) is metered in elec_w, but its heat goes straight into
            the tank, not into heat_w. The controller stops space heating above a 15 degC
            daily mean outdoors, and idles below 15 % of its rating ("standby").
pv          A PV inverter, JSON every 60 s, QoS 0: {"ts": ms, "ac_w": 412, "dc_w": 437,
            "today_kwh": 17.42, "state": "MPPT" | "Sleeping"}. Output is clear-sky
            irradiance on the array's plane (solar position computed for the actual time),
            times a slowly drifting cloud factor: in late March at 17:00 the sun is ~12 deg
            up in the west-south-west and setting at ~18:30, so output is low and falling.
battery     A home battery's hybrid inverter, JSON every 60 s, QoS 0: {"ts": ms,
            "soc_pct": 71.4, "power_w": -1450, "mode": "hold" | "peak-shaving",
            "temp_c": 17.9}. power_w > 0 charges, < 0 discharges. It charges from PV surplus
            at any time, holds its charge until 17:00 local, then discharges to cover the
            house's own load (not the EV) through the 17:00-21:00 peak, down to 10 % SoC.
evcharger   An EV charger speaking OCPP 1.6J. Its local OCPP-to-MQTT bridge publishes each
            call the charger makes as its action name plus the call's payload, QoS 1:
            - a StatusNotification on every status change (and the current one when the
              bridge starts), retained: {"action": "StatusNotification", "connectorId": 1,
              "status": "Available" | "Preparing" | "Charging" | "SuspendedEV" |
              "SuspendedEVSE" | "Finishing", "errorCode": "NoError",
              "timestamp": "2026-03-24T16:00:04.700Z"};
            - a StartTransaction when a session starts (an RFID card presented to a
              plugged-in car): {"action": "StartTransaction", "connectorId": 1,
              "idTag": "04E1A35A7C6B80", "meterStart": 6184220,
              "timestamp": "2026-03-24T16:00:04.500Z"} (meterStart in Wh);
            - MeterValues every 60 s while a transaction is open, in OCPP's own shape:
              {"action": "MeterValues", "connectorId": 1, "transactionId": 4711,
               "meterValue": [{"timestamp": ..., "sampledValue": [
                 {"value": "11027", "context": "Sample.Periodic",
                  "measurand": "Power.Active.Import", "unit": "W"},
                 {"value": "6184391", "context": "Sample.Periodic",
                  "measurand": "Energy.Active.Import.Register", "unit": "Wh"}]}]}
              (values are strings, as OCPP sends them; the register is the charger's
              lifetime energy, not the session's).
            OCPP timestamps are RFC 3339 UTC strings, not milliseconds.

Every JSON "ts" is the device's own clock, Unix milliseconds (core.ts_ms).

The households (times for --start 2026-03-24T15:55:00Z, i.e. 16:55 CET on a Tuesday):

hh-104  Villa. Heat pump (3-phase, radiators), 6.2 kWp PV facing south-west, 10 kWh / 5 kW
        battery, 11 kW EV charger. The car is plugged in at t = 281 s; the session starts
        (StartTransaction) at t = 304.5 s and it charges from t = 305 s: 17:00:05, five
        seconds into the peak tariff.
hh-117  1930s brick house, heat pump on radiators. Its circulation pump is failing: since
        early morning the heat pump has kept tripping on high pressure and delivered about
        60 % of the heat the house needs, so the living room has drifted down to 19.4 degC,
        2.1 K under its 21.5 degC setpoint. At t = 88 s it trips once too often and locks
        out (compressor off, fault "E35 high-pressure lockout", fault_since 15:56:28Z in
        the fixture). The weak pump still
        circulates, so flow and return read the same and fall together as the radiators
        give up their heat to the rooms (an hour's time constant). The room follows a heat
        balance with the house's 30 h time constant, so it cools by 0.15-0.4 K an hour:
        the cold room is the state of the house, not a quick consequence of the lockout.
        Outside the heating season (the heat pump would be idle) there is nothing to trip
        on: hh-117 then runs normally at its setpoint.
hh-123  New build, underfloor heating. The heat pump's evaporator is icing up (a refrigerant
        undercharge; the defrost never clears it): at a mild 6 degC outdoors its COP
        sinks from 2.4 to 1.55 over the run, below 2.0 from about t = 195 s.
hh-131  Terraced house on district heating, 4.1 kWp PV. Its EV has charged since 16:30; the
        charger's smart schedule pauses it at 17:00 (SuspendedEVSE). The thermostat
        schedule raises the setpoint from 20 to 21 degC at 17:00: a 1 K step, which must
        not look like a heating failure.
hh-142  Farmhouse at the far end of a long rural LV feeder: voltages run at 215-219 V. Heat
        pump, 8.4 kWp PV facing west (exporting at the start). At t = 210-450 s the heat
        pump defrosts (heat_w < 0: a negative COP that is normal and must not raise an
        alarm). From t = 290 s (16:59:50 in the fixture) the evening load along the
        feeder (the neighbouring farms' heat pumps and cookers, and a car charging on L2)
        pulls phase L2 down to 203.5-205.5 V and holds it there for the rest of the run:
        a sustained undervoltage, below EN 50160's 207 V (230 V - 10 %), the kind the
        standard's 10-minute means catch (a fall below 207 V that lasts under a minute
        would be a dip, which the standard only characterises). L1 and L3 sag to 212-215 V
        and stay inside. The load is on the simulation's clock, so it comes in every run;
        at midday in summer, PV export lifts hh-142's voltages by ~2 V, and L2 hovers
        around 207 V, in and out of the band.
hh-158  House on district heating; a plug-in hybrid charges at 3.7 kW on one phase, tapers
        off as its battery fills and stops at t = 470 s (SuspendedEV).
hh-163  Summer house, empty: thermostat in "away" mode at 16 degC, heat pump at minimum,
        5.0 kWp PV, 7 kWh / 3.5 kW battery.
hh-170  Flat on district heating, P1 reader only besides the thermostat.
hh-186  Heat pump that heats the hot-water tank at t = 150-480 s (mode "dhw": high flow
        temperature, lower COP, still normal). It is the weekly legionella cycle: from
        t = 390 s the 3 kW immersion heater lifts the tank to 65 degC, and the measured COP
        falls to ~1.05, which a rule that ignored the mode would alarm on. EV charger with
        no car (Available). No P1 reader.
hh-191  District heating, 3.0 kWp PV on one phase. No P1 reader.
hh-205  District heating, thermostat only.
hh-212  Flat on district heating, thermostat only.

In a 600 s run that is 480 P1 telegrams, 120 thermostat, 120 heat-pump, 50 PV, 20
battery and 33 EV-charger messages: 823 in all.

What the homes section of demo/rules/rules.toml should make of that run (--seed 7, the start above): 771
derived messages, of which 120 are retained heat-pump states and 11 are alerts, every one
from a fault above (5 once-a-minute reminders of hh-142's L2 undervoltage, 2 cold-room
reminders at hh-117, hh-117's E35 lockout as it happens and once more 4 minutes later,
hh-123's low COP, hh-104's session started in the peak; the rules re-alert a lasting
condition at most every minute or every 5 minutes), and none from the look-alikes:
hh-142's defrost (COP < 0), hh-186's legionella cycle (COP 1.05 in "dhw" mode), hh-117's
degraded but running heat pump before its lockout (COP 2.2), hh-131's 1 K setpoint step,
hh-163's cold "away" house, hh-131's and hh-158's sessions that started before 17:00
(re-announced as "Charging" when the bridges start, and hh-131 pausing at 17:00), and the
MeterValues through the peak.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

from .core import Event, jbytes, ts_ms

LAT, LON = 55.64, 12.09  # Roskilde, Zealand
WATER_CP = 4186.0  # J/(kg K); a litre of water is ~1 kg
STEP = 2.0  # s: the integration step for meter registers, SoC, room temperature
TAU = 2 * math.pi
GAINS_W = 350.0  # a lived-in house's internal gains (people, appliances, lights), W
HEATING_LIMIT_C = 15.0  # degC daily mean outdoors above which heat pumps stop space heating
TRIP_AT = 88.0  # s: hh-117's heat pump locks out
HOUSE_TAU = 30 * 3600.0  # s: hh-117's time constant (heat capacity / heat-loss coefficient)
LOOP_TAU = 3600.0  # s: how fast hh-117's radiators and pipework cool once nothing heats them

# Danish monthly mean temperatures, degC (DMI 1991-2020 national normals, rounded)
NORMALS = [1.6, 1.6, 3.4, 7.0, 11.0, 14.5, 17.1, 17.0, 13.9, 9.6, 5.7, 2.8]


# ---- time and sun ----------------------------------------------------------------------


def dk_offset_s(unix: float) -> int:
    """Denmark's UTC offset in seconds: CEST from 01:00 UTC on the last Sunday of March to
    01:00 UTC on the last Sunday of October (the EU rule), CET otherwise."""
    year = datetime.fromtimestamp(unix, timezone.utc).year

    def last_sunday(month: int) -> float:
        d = datetime(year, month, 31, 1, tzinfo=timezone.utc)
        return (d - timedelta(days=(d.weekday() + 1) % 7)).timestamp()

    return 7200 if last_sunday(3) <= unix < last_sunday(10) else 3600


def local_time(unix: float) -> datetime:
    """Danish wall-clock time (naive)."""
    return datetime.fromtimestamp(math.floor(unix) + dk_offset_s(unix), timezone.utc).replace(tzinfo=None)


def local_to_unix(lt: datetime) -> float:
    """The Unix time of a Danish wall-clock time (naive), away from the hour a DST switch
    skips or repeats."""
    naive = lt.replace(tzinfo=timezone.utc).timestamp()
    return naive - dk_offset_s(naive - 3600)


def local_hour(unix: float) -> float:
    lt = local_time(unix)
    return lt.hour + lt.minute / 60 + lt.second / 3600


def sun(unix: float) -> Tuple[float, float]:
    """Solar elevation and azimuth in degrees (azimuth clockwise from north), from the
    low-precision formulas of the Astronomical Almanac (good to ~0.5 deg)."""
    d = unix / 86400.0 - 10957.5  # days since J2000.0
    g = math.radians((357.529 + 0.98560028 * d) % 360)
    q = (280.459 + 0.98564736 * d) % 360
    lam = math.radians(q + 1.915 * math.sin(g) + 0.020 * math.sin(2 * g))
    eps = math.radians(23.439 - 0.00000036 * d)
    ra = math.atan2(math.cos(eps) * math.sin(lam), math.cos(lam))
    dec = math.asin(math.sin(eps) * math.sin(lam))
    ha = math.radians(((18.697374558 + 24.06570982441908 * d) % 24) * 15 + LON) - ra
    phi = math.radians(LAT)
    elev = math.asin(math.sin(phi) * math.sin(dec) + math.cos(phi) * math.cos(dec) * math.cos(ha))
    az = math.atan2(-math.cos(dec) * math.sin(ha),
                    math.sin(dec) * math.cos(phi) - math.cos(dec) * math.cos(ha) * math.sin(phi))
    return math.degrees(elev), math.degrees(az) % 360


def plane_irradiance(unix: float, tilt: float, facing: float) -> float:
    """Clear-sky irradiance on a tilted plane, W/m2: Meinel's beam model with the
    Kasten-Young air mass, a simple diffuse share and ground reflection."""
    elev, az = sun(unix)
    if elev <= 0.5:
        return 0.0
    e = math.radians(elev)
    air_mass = 1 / (math.sin(e) + 0.50572 * (elev + 6.07995) ** -1.6364)
    beam = 1353 * 0.7 ** (air_mass ** 0.678)
    diffuse = 0.12 * beam + 15
    t = math.radians(tilt)
    cos_inc = math.sin(e) * math.cos(t) + math.cos(e) * math.sin(t) * math.cos(math.radians(az - facing))
    ground = (beam * math.sin(e) + diffuse) * 0.2 * (1 - math.cos(t)) / 2
    return beam * max(cos_inc, 0.0) + diffuse * (1 + math.cos(t)) / 2 + ground


def vapour_pressure(t_c: float) -> float:
    """Saturation vapour pressure over water, hPa (the Magnus formula, WMO coefficients)."""
    return 6.112 * math.exp(17.62 * t_c / (243.12 + t_c))


def smoothstep(x: float) -> float:
    x = min(max(x, 0.0), 1.0)
    return x * x * (3 - 2 * x)


def window(t: float, start: float, end: float, ramp_in: float, ramp_out: float) -> float:
    """0 outside [start, end], 1 inside, with smooth ramps of the given lengths."""
    return smoothstep((t - start) / ramp_in) * (1 - smoothstep((t - end) / ramp_out))


class Wobble:
    """Smooth, deterministic variation: a sum of three sines with random periods and phases."""

    def __init__(self, rng: random.Random, amp: float, periods: Tuple[float, float]):
        self.terms = [(amp * w, TAU / rng.uniform(*periods), rng.uniform(0, TAU)) for w in (0.6, 0.3, 0.15)]

    def __call__(self, t: float) -> float:
        return sum(a * math.sin(w * t + p) for a, w, p in self.terms)


# ---- weather -----------------------------------------------------------------------------


class Weather:
    """One weather for all the homes (they lie within ~30 km of Roskilde)."""

    def __init__(self, rng: random.Random, t0: float):
        self.t0 = t0
        self.anomaly = -1.6 + rng.uniform(-0.3, 0.3)  # a cool, breezy evening
        self.temp_drift = Wobble(rng, 0.25, (900, 3600))
        self.cloud_base = rng.uniform(0.6, 0.7)  # thin high cloud
        self.cloud_drift = Wobble(rng, 0.035, (240, 1500))

    def daily_mean_c(self, t: float) -> float:
        """Today's mean outdoor temperature: the monthly normals, interpolated between
        mid-months, plus this spell's anomaly."""
        unix = self.t0 + t
        doy = local_time(unix).timetuple().tm_yday + local_hour(unix) / 24
        m = (doy - 15.5) / 30.44
        i = math.floor(m)
        return NORMALS[i % 12] + (NORMALS[(i + 1) % 12] - NORMALS[i % 12]) * (m - i) + self.anomaly

    def outdoor_c(self, t: float) -> float:
        diurnal = 3.0 * math.cos(TAU * (local_hour(self.t0 + t) - 15.0) / 24)  # warmest at 15:00
        return self.daily_mean_c(t) + diurnal + self.temp_drift(t)

    def clear_fraction(self, t: float, lag: float) -> float:
        """The share of clear-sky irradiance getting through the clouds, drifting as they pass."""
        return min(max(self.cloud_base + self.cloud_drift(t + lag), 0.15), 1.0)


# ---- the devices' physics ------------------------------------------------------------------


@dataclass
class Appliance:
    start: float
    end: float
    watts: float
    phase: int
    cycle: float = 0.0  # an oven's thermostat: full power for 10 min, then on 1/3 of each cycle

    def power(self, t: float) -> float:
        if not self.start <= t < self.end:
            return 0.0
        if self.cycle and t - self.start > 600:
            return self.watts if (t - self.start) % self.cycle < self.cycle / 3 else 0.0
        return self.watts


# watts (lo, hi), seconds (lo, hi), starts per hour in the evening (16:30-19:30), at other times
APPLIANCES = {
    "kettle": ((1900, 2200), (110, 220), 0.6, 0.15),
    "hob": ((1100, 2300), (480, 1500), 0.7, 0.04),
    "oven": ((2100, 2500), (1800, 3300), 0.3, 0.02),
    "microwave": ((800, 1150), (60, 240), 0.4, 0.08),
}


class Household:
    """The house's own load: standby, the fridge's compressor, lights after dusk, cooking."""

    def __init__(self, rng: random.Random, t0: float, duration: float, size: float, occupied: bool):
        self.t0 = t0
        self.standby = [rng.uniform(40, 110) * size for _ in range(3)]
        self.fridge = (rng.uniform(75, 110), rng.uniform(2100, 2700), rng.uniform(0, 2700), rng.randrange(3))
        self.lights = rng.uniform(120, 320) * size if occupied else 0.0
        self.jitter = Wobble(rng, 25 * size, (20, 200))
        self.appliances: List[Appliance] = []
        if occupied:
            for name, ((wlo, whi), (dlo, dhi), evening, other) in APPLIANCES.items():
                t = -3600.0  # things already switched on at the start
                while True:
                    t += rng.expovariate(max(evening, other) * size / 3600)
                    if t >= duration:
                        break
                    h = local_hour(t0 + t)
                    rate = evening if 16.5 <= h < 19.5 else other
                    if rng.random() < rate / max(evening, other):
                        d = rng.uniform(dlo, dhi)
                        cycle = rng.uniform(150, 240) if name == "oven" else 0.0
                        self.appliances.append(Appliance(t, t + d, rng.uniform(wlo, whi), rng.randrange(3), cycle))

    def phases(self, t: float) -> List[float]:
        out = list(self.standby)
        watts, period, offset, phase = self.fridge
        if (t + offset) % period < 0.35 * period:
            out[phase] += watts
        elev, _ = sun(self.t0 + t)
        dusk = min(max((6.0 - elev) / 6.0, 0.0), 1.0)
        out[0] += 0.6 * self.lights * dusk
        out[1] += 0.4 * self.lights * dusk
        for a in self.appliances:
            out[a.phase] += a.power(t)
        out[2] += self.jitter(t)
        return [max(p, 0.0) for p in out]


class HeatPump:
    """An air-to-water heat pump, its heating curve, COP and the scenario it plays."""

    def __init__(self, rng: random.Random, weather: Weather, *, nominal_w: float, ua: float, room_c: float,
                 curve: Tuple[float, float], lpm: float, three_phase: bool, scenario: str = "normal",
                 outdoor_bias: float = 0.0):
        self.weather = weather
        self.nominal_w, self.ua, self.room_c = nominal_w, ua, room_c
        self.curve = curve  # flow temperature = base + slope * (20 - outdoor)
        self.lpm, self.three_phase, self.scenario = lpm, three_phase, scenario
        self.outdoor_bias = outdoor_bias
        self.eta = rng.uniform(0.47, 0.51)  # share of the Carnot COP this unit reaches
        self.demand_drift = Wobble(rng, 0.04, (120, 900))
        self.lpm_drift = Wobble(rng, 0.15, (60, 300))
        self.room_ref = room_c  # hh-117: the room its radiators cool towards (set by Home)

    def outdoor(self, t: float) -> float:
        return self.weather.outdoor_c(t) + self.outdoor_bias

    def cop(self, flow_c: float, outdoor_c: float) -> float:
        cond, evap = flow_c + 2.5 + 273.15, outdoor_c - 7.0 + 273.15
        return self.eta * cond / (cond - evap)

    def demand(self, t: float) -> float:
        """The heat the house needs at its design temperature, W."""
        return (self.ua * (self.room_c - self.outdoor(t)) - GAINS_W) * (1 + self.demand_drift(t))

    def heating_season(self, t: float) -> bool:
        """Whether the unit heats the house at all. Its controller stops space heating above a
        15 degC daily mean outdoors (the usual factory heating limit), and below 15 % of its
        rating it idles in standby."""
        return (self.weather.daily_mean_c(t) + self.outdoor_bias < HEATING_LIMIT_C
                and self.demand(t) >= 0.15 * self.nominal_w)

    def degraded_heat(self, t: float) -> float:
        """hh-117 before the lockout: tripping and restarting, it delivers ~60 % of the demand."""
        return 0.58 * self.demand(t)

    def emitted_w(self, t: float) -> float:
        """hh-117: the heat its radiators give the rooms, W. After the lockout the loop's water
        cools through them, so their output decays rather than stopping."""
        if t < TRIP_AT:
            return self.degraded_heat(t)
        return self.degraded_heat(TRIP_AT) * math.exp(-(t - TRIP_AT) / LOOP_TAU)

    def state(self, t: float) -> dict:
        out = self.outdoor(t)
        base, slope = self.curve
        flow_set = min(max(base + slope * (20 - out), 25.0), 55.0)
        s = {"mode": "heat", "compressor": True, "defrost": False, "fault": "", "fault_since": None, "outdoor_c": out,
             "lpm": self.lpm + self.lpm_drift(t)}
        sc = self.scenario
        if sc == "lockout" and t >= TRIP_AT:
            # hh-117, locked out: compressor off; the failing pump still circulates, and with
            # no heat source the water leaves the unit as it came back (a little cooler: the
            # monobloc stands outdoors) while the whole loop cools towards the room.
            q = self.degraded_heat(TRIP_AT)
            ret_trip = 47.5 - q / ((4.6 + 0.1 * self.lpm_drift(TRIP_AT)) / 60 * WATER_CP)
            ret = self.room_ref + (ret_trip - self.room_ref) * math.exp(-(t - TRIP_AT) / LOOP_TAU)
            return dict(s, compressor=False, hz=0, elec=38.0, heat=0.0, flow=ret - 0.1, ret=ret, lpm=4.2,
                        fault="E35 high-pressure lockout", fault_since=TRIP_AT)
        demand = self.demand(t)
        if not self.heating_season(t):
            return dict(s, mode="standby", compressor=False, hz=0, elec=14.0, heat=0.0,
                        flow=self.room_c + 2.0, ret=self.room_c + 1.6)
        heat, flow = min(demand, self.nominal_w), flow_set
        cop = self.cop(flow, out)
        booster = 0.0
        ice = smoothstep((t + 100) / 560)
        if sc == "lockout":  # hh-117 before the lockout: failing circulation pump, high-pressure trips
            s["lpm"] = 4.6 + 0.1 * self.lpm_drift(t)
            heat, flow, cop = self.degraded_heat(t), 47.5, 2.33
        elif sc == "icing":  # hh-123: the evaporator ices; capacity and COP fall
            cop = 2.62 - 1.0 * ice
            heat = demand * (1 - 0.22 * ice)
        elif sc == "defrost" and 210 <= t < 450:  # hh-142: a reverse-cycle defrost
            # The controller raises the defrost flag as it reverses the cycle, before the heat
            # flow has turned round, and keeps it up until the coil is clear.
            k = window(t, 210, 450, 25, 1)
            flow = flow_set - 5.5 * k
            elec = 1420 * (1 + self.demand_drift(t))
            heat = -1850 * k * (1 + 0.8 * self.demand_drift(t + 211.0)) + heat * (1 - k)
            ret = flow - heat / (s["lpm"] / 60 * WATER_CP)
            return dict(s, defrost=True, hz=72, elec=elec, heat=heat, flow=flow, ret=ret)
        elif sc == "defrost" and 450 <= t < 570:  # recovering: running harder for a while
            heat *= 1.0 + 0.25 * (1 - (t - 450) / 120)
        elif sc == "dhw" and 150 <= t < 480:  # hh-186: heating the hot-water tank
            k = smoothstep((t - 150) / 300)
            flow = 38.0 + 15.0 * k
            heat = 5200 * (1 + self.demand_drift(t))
            cop = self.cop(flow, out)
            s["mode"] = "dhw"
            if t >= 390:  # the weekly legionella cycle: the 3 kW immersion heater lifts the tank to 65 degC
                booster = 3000.0
        elec = heat / cop + 40 + 15 + booster  # + circulation pump and controls (+ immersion heater)
        ret = flow - heat / (s["lpm"] / 60 * WATER_CP)
        hz = round(min(max(25 + 60 * heat / self.nominal_w, 20), 90))
        if sc == "icing":
            hz = round(78 + 6 * ice)
        return dict(s, hz=hz, elec=elec, heat=heat, flow=flow, ret=ret)

    def phase_watts(self, elec: float) -> List[float]:
        return [elec / 3] * 3 if self.three_phase else [0.0, 0.0, elec]


class PV:
    """A PV array and its inverter."""

    def __init__(self, rng: random.Random, weather: Weather, t0: float, kwp: float, facing: float,
                 three_phase: bool):
        self.weather, self.t0, self.kwp, self.facing, self.three_phase = weather, t0, kwp, facing, three_phase
        self.tilt = rng.uniform(25, 40)
        self.lag = rng.uniform(0, 400)  # the clouds reach each roof at a different moment
        self.pr = rng.uniform(0.82, 0.88)  # performance ratio
        # what it has produced since local midnight, at the start
        midnight = t0 - local_hour(t0) * 3600
        self.today_kwh = sum(self.ac_w(midnight + q * 900 - t0) for q in range(int(local_hour(t0) * 4))) * 0.25 / 1000

    def ac_w(self, t: float) -> float:
        g = plane_irradiance(self.t0 + t, self.tilt, self.facing) * self.weather.clear_fraction(t, self.lag)
        w = self.kwp * g * self.pr
        return w if w > 8 else 0.0

    def phase_watts(self, w: float) -> List[float]:
        return [w / 3] * 3 if self.three_phase else [0.0, w, 0.0]


class Battery:
    """A home battery behind a hybrid inverter (3-phase)."""

    def __init__(self, rng: random.Random, kwh: float, kw: float, soc: float):
        self.kwh, self.max_w, self.soc = kwh, kw * 1000, soc
        self.temp_c = rng.uniform(14.5, 19.0)
        self.tracking = Wobble(rng, 45.0, (8, 60))  # control error: it follows the load with a lag

    def mode(self, unix: float) -> str:
        return "peak-shaving" if 17 <= local_hour(unix) < 21 else "hold"

    def power(self, unix: float, house_w: float, pv_w: float) -> float:
        """The inverter's setpoint: > 0 charges, < 0 discharges."""
        surplus = pv_w - house_w
        err = self.tracking(unix)
        if surplus > 0 and self.soc < 100:
            return max(min(surplus + err - 30, self.max_w), 0.0)
        if self.mode(unix) == "peak-shaving" and self.soc > 10:
            return -max(min(-surplus + err - 30, self.max_w), 0.0)
        return 0.0

    def integrate(self, w: float, dt: float):
        eff = 0.95 if w > 0 else 1 / 0.95
        self.soc = min(max(self.soc + w * eff * dt / 3600 / (self.kwh * 1000) * 100, 0.0), 100.0)


@dataclass
class EvCharger:
    """An OCPP charger and the session it plays: a list of (t, status) changes plus a power
    profile. Sessions that run at the start began before it. register_wh is the charger's
    lifetime energy register (OCPP's Energy.Active.Import.Register), id_tag the RFID card
    that starts a new session."""

    scenario: str
    tx: int
    phases: Tuple[int, ...]
    register_wh: float
    id_tag: str
    changes: List[Tuple[float, str]] = field(default_factory=list)

    def status(self, t: float) -> str:
        current = self.changes[0][1]
        for at, st in self.changes:
            if at <= t:
                current = st
        return current

    def power_w(self, t: float) -> float:
        st = self.status(t)
        if st != "Charging":
            return 0.0
        if self.scenario == "peak-start":  # hh-104: 3 x 16 A, ramping up over 8 s
            return 11040 * smoothstep((t - 304.7) / 8) * (0.995 + 0.004 * math.sin(t / 7))
        if self.scenario == "smart-pause":  # hh-131
            return 11000 * (0.995 + 0.004 * math.sin(t / 9))
        if self.scenario == "phev-full":  # hh-158: 1 x 16 A, tapering as the pack fills
            return 3680 if t < 380 else 3680 - 3080 * smoothstep((t - 380) / 90)
        return 0.0

    def transaction_open(self, t: float) -> bool:
        return self.status(t) in ("Charging", "SuspendedEV", "SuspendedEVSE")

    def session_starts(self) -> List[float]:
        """When a new session starts: a plugged-in car (Preparing) that begins Charging."""
        return [at for (_, before), (at, st) in zip(self.changes, self.changes[1:])
                if before == "Preparing" and st == "Charging"]


class Meter:
    """The smart meter: its identity, registers, counters, and the DSMR 5.0 telegram."""

    def __init__(self, rng: random.Random, has_pv: bool):
        digits = lambda n: "".join(str(rng.randrange(10)) for _ in range(n))  # noqa: E731
        self.ident = "/XMX5LGBBLB" + digits(10)
        self.equipment = "E00" + digits(14)
        imp1 = rng.uniform(2600, 6200)
        exp1 = rng.uniform(700, 2400) if has_pv else 0.0
        # [delivered tariff 1, tariff 2, returned tariff 1, tariff 2], kWh
        self.reg = [imp1, imp1 * rng.uniform(1.0, 1.35), exp1, exp1 * rng.uniform(1.6, 2.4)]
        self.failures = rng.randint(3, 9)
        self.long_failures = rng.randint(1, 3)
        log = []
        for _ in range(self.long_failures):  # power cuts last autumn and winter: (end time, duration)
            wall = datetime(2025, rng.choice([1, 2, 3, 10, 11, 12]), rng.randint(1, 28), rng.randint(0, 23),
                            rng.randint(0, 59), rng.randint(0, 59), tzinfo=timezone.utc).timestamp()
            # the meter stamps local time and flags it S (summer, UTC+2) or W (winter, UTC+1)
            summer = dk_offset_s(wall - 7200) == 7200
            when = datetime.fromtimestamp(wall, timezone.utc).strftime("%y%m%d%H%M%S") + ("S" if summer else "W")
            log.append((when, f"({when})({rng.randint(185, 5400):010d}*s)"))
        self.failure_log = f"1-0:99.97.0({len(log)})(0-0:96.7.19)" + "".join(e for _, e in sorted(log))
        self.sags = [rng.randint(0, 3) for _ in range(3)]
        self.swells = [rng.randint(0, 1) for _ in range(3)]
        self.below = [False, False, False]  # for counting sags

    @staticmethod
    def tariff(unix: float) -> int:
        lt = local_time(unix)
        return 2 if lt.weekday() < 5 and 7 <= lt.hour < 23 else 1

    def integrate(self, net_w: float, dt: float, unix: float):
        k = self.tariff(unix) - 1
        if net_w >= 0:
            self.reg[k] += net_w * dt / 3.6e6
        else:
            self.reg[2 + k] -= net_w * dt / 3.6e6

    def telegram(self, unix: float, phase_w: List[float], volts: List[float]) -> bytes:
        for i, v in enumerate(volts):  # the meter counts a sag each time a phase drops below 207 V
            if v < 207.0 and not self.below[i]:
                self.sags[i] += 1
            self.below[i] = v < 207.0
        lt = local_time(unix)
        clock = lt.strftime("%y%m%d%H%M%S") + ("S" if dk_offset_s(unix) == 7200 else "W")
        net = sum(phase_w)
        amps = [round(abs(w) / v) for w, v in zip(phase_w, volts)]
        kw = lambda w: f"{max(round(w / 1000, 3), 0.0) + 0.0:06.3f}"  # noqa: E731  (never "-0.000")
        lines = [
            self.ident,
            "",
            "1-3:0.2.8(50)",
            f"0-0:1.0.0({clock})",
            f"0-0:96.1.1({self.equipment.encode('ascii').hex().upper()})",
            f"1-0:1.8.1({self.reg[0]:010.3f}*kWh)",
            f"1-0:1.8.2({self.reg[1]:010.3f}*kWh)",
            f"1-0:2.8.1({self.reg[2]:010.3f}*kWh)",
            f"1-0:2.8.2({self.reg[3]:010.3f}*kWh)",
            f"0-0:96.14.0({self.tariff(unix):04d})",
            f"1-0:1.7.0({kw(max(net, 0))}*kW)",
            f"1-0:2.7.0({kw(max(-net, 0))}*kW)",
            f"0-0:96.7.21({self.failures:05d})",
            f"0-0:96.7.9({self.long_failures:05d})",
            self.failure_log,
            f"1-0:32.32.0({self.sags[0]:05d})",
            f"1-0:52.32.0({self.sags[1]:05d})",
            f"1-0:72.32.0({self.sags[2]:05d})",
            f"1-0:32.36.0({self.swells[0]:05d})",
            f"1-0:52.36.0({self.swells[1]:05d})",
            f"1-0:72.36.0({self.swells[2]:05d})",
            "0-0:96.13.0()",
            f"1-0:32.7.0({volts[0]:05.1f}*V)",
            f"1-0:52.7.0({volts[1]:05.1f}*V)",
            f"1-0:72.7.0({volts[2]:05.1f}*V)",
            f"1-0:31.7.0({amps[0]:03d}*A)",
            f"1-0:51.7.0({amps[1]:03d}*A)",
            f"1-0:71.7.0({amps[2]:03d}*A)",
            f"1-0:21.7.0({kw(max(phase_w[0], 0))}*kW)",
            f"1-0:41.7.0({kw(max(phase_w[1], 0))}*kW)",
            f"1-0:61.7.0({kw(max(phase_w[2], 0))}*kW)",
            f"1-0:22.7.0({kw(max(-phase_w[0], 0))}*kW)",
            f"1-0:42.7.0({kw(max(-phase_w[1], 0))}*kW)",
            f"1-0:62.7.0({kw(max(-phase_w[2], 0))}*kW)",
        ]
        body = "\r\n".join(lines) + "\r\n!"
        return (body + f"{crc16(body.encode('ascii')):04X}\r\n").encode("ascii")


def crc16(data: bytes) -> int:
    """CRC-16/ARC (polynomial 0x8005, reflected, initial value 0), as DSMR 5.0 specifies."""
    crc = 0
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


# ---- a home --------------------------------------------------------------------------------


@dataclass
class Sample:
    at: float
    device: str
    order: int  # breaks ties between devices that sample at the same instant


class Home:
    def __init__(self, rng: random.Random, t0: float, duration: float, weather: Weather, home: str, *,
                 size: float = 1.0, occupied: bool = True, p1: bool = True,
                 volts: Tuple[float, float, float] = (234.0, 235.0, 233.5), feeder_ohm: float = 0.18,
                 setpoint: float = 21.0, mode: str = "heat", room_bias: float = 0.0, comfort_step: bool = False,
                 hp: Optional[dict] = None, pv: Optional[Tuple[float, float, bool]] = None,
                 battery: Optional[Tuple[float, float, float]] = None, ev: Optional[EvCharger] = None,
                 room_start: Optional[float] = None, sag: Optional[Tuple[float, float, float]] = None):
        self.rng, self.t0, self.duration, self.weather, self.home = rng, t0, duration, weather, home
        self.house = Household(rng, t0, duration, size, occupied)
        self.meter = Meter(rng, pv is not None) if p1 else None
        self.v0, self.feeder_ohm, self.sag = list(volts), feeder_ohm, sag
        self.v_drift = Wobble(rng, 0.9, (90, 900))
        self.setpoint, self.mode, self.comfort_step = setpoint, mode, comfort_step
        self.room_bias = room_bias
        self.room_swing = Wobble(rng, 0.12 if hp else 0.22, (600, 1500))
        self.humidity = rng.uniform(34, 47)  # relative humidity, %, at the starting room temperature
        self.hp = HeatPump(rng, weather, **hp) if hp else None
        if self.hp and self.hp.scenario == "lockout" and not self.hp.heating_season(0.0):
            # Outside the heating season the heat pump idles: nothing to trip on, nothing to show.
            self.hp.scenario, room_start = "normal", None
        self.room = room_start if room_start is not None else self.setpoint_at(0.0) + room_bias
        self.room_at_start = self.room
        if self.hp:
            self.hp.room_ref = self.room
        self.pv = PV(rng, weather, t0, *pv) if pv else None
        self.battery = Battery(rng, *battery) if battery else None
        self.ev = ev

    # -- behaviour

    def setpoint_at(self, t: float) -> float:
        if not self.comfort_step:
            return self.setpoint
        h = local_hour(self.t0 + t)  # hh-131: 21 degC 06-08 and 17-22:30, 20 degC otherwise
        return self.setpoint + 1.0 if (6 <= h < 8 or 17 <= h < 22.5) else self.setpoint

    def setpoint_since(self, t: float) -> float:
        """When the current setpoint took effect, Unix s: for hh-131's schedule the start of
        the current slot, for the other homes the owner's last change, at 18:00 on 1 October
        when the heating season began."""
        lt = local_time(self.t0 + t)
        if self.comfort_step:
            starts = [lt.replace(hour=h, minute=m, second=0) - timedelta(days=back)
                      for back in (0, 1) for h, m in ((6, 0), (8, 0), (17, 0), (22, 30))]
            return local_to_unix(max(s for s in starts if s <= lt))
        start = lt.replace(month=10, day=1, hour=18, minute=0, second=0)
        return local_to_unix(start if start <= lt else start.replace(year=lt.year - 1))

    def powers(self, t: float):
        """Every load and source in the house at t, W per phase: house, heat pump, EV, PV, battery."""
        house = self.house.phases(t)
        hp_state = self.hp.state(t) if self.hp else None
        hp = self.hp.phase_watts(hp_state["elec"]) if hp_state else [0.0] * 3
        ev_w = self.ev.power_w(t) if self.ev else 0.0
        ev = [ev_w / len(self.ev.phases) if i in self.ev.phases else 0.0 for i in range(3)] if self.ev else [0.0] * 3
        pv_w = self.pv.ac_w(t) if self.pv else 0.0
        pv = self.pv.phase_watts(pv_w) if self.pv else [0.0] * 3
        batt_w = self.battery.power(self.t0 + t, sum(house) + sum(hp), pv_w) if self.battery else 0.0
        net = [house[i] + hp[i] + ev[i] - pv[i] + batt_w / 3 for i in range(3)]
        return net, hp_state, ev_w, pv_w, batt_w

    def advance(self, t_from: float, t_to: float):
        """Integrate the meter registers, battery SoC, EV session energy, PV yield and room
        temperature from t_from to t_to."""
        t = t_from
        while t < t_to:
            dt = min(STEP, t_to - t)
            mid = t + dt / 2
            net, _, ev_w, pv_w, batt_w = self.powers(mid)
            if self.meter:
                self.meter.integrate(sum(net), dt, self.t0 + mid)
            if self.battery:
                self.battery.integrate(batt_w, dt)
            if self.ev:
                self.ev.register_wh += ev_w * dt / 3600
            if self.pv:
                self.pv.today_kwh += pv_w * dt / 3.6e6
            self.room += self.room_rate(mid) * dt
            t += dt

    def room_rate(self, t: float) -> float:
        """degC per second."""
        if self.hp and self.hp.scenario == "lockout":
            # hh-117, a heat balance: what the radiators give, plus the internal gains, minus
            # what the envelope loses, over the house's heat capacity (a 30 h time constant:
            # a heavy brick house cools by tenths of a degree an hour, not degrees).
            hp = self.hp
            loss = hp.ua * (self.room - self.weather.outdoor_c(t))
            return (hp.emitted_w(t) + GAINS_W - loss) / (hp.ua * HOUSE_TAU)
        target = self.setpoint_at(t) + self.room_bias
        rate = (target - self.room) / 1800  # settles in about half an hour...
        return min(max(rate, -0.8 / 3600), 1.2 / 3600)  # ...within what the heating can do

    def voltages(self, t: float, net: List[float]) -> List[float]:
        evening = -1.5 * smoothstep((local_hour(self.t0 + t) - 16.75) / 0.5)  # the 17:00 load rise
        out = []
        for i in range(3):
            v = self.v0[i] + self.v_drift(t + 37 * i) + evening - self.feeder_ohm * net[i] / 230
            if self.sag:
                start, end, depth = self.sag
                v -= depth * (1.0, 2.25, 0.85)[i] * window(t, start, end, 12, 18)
            out.append(round(v + self.rng.gauss(0, 0.2), 1))
        return out

    # -- the schedule and the messages

    def schedule(self) -> List[Sample]:
        rng, d = self.rng, self.duration
        out: List[Sample] = []

        def every(device: str, period: float, phase: float, order: int, start: float = 0.0, end: float = d):
            t = start + phase
            while t < min(end, d):
                out.append(Sample(t, device, order))
                t += period

        if self.meter:  # published up to 0.6 s after the meter's clock tick: keep that inside the run
            every("p1", 10.0, float(rng.randrange(10)), 0, end=d - 0.6)
        every("thermostat", 60.0, rng.uniform(0, 60), 1)
        if self.hp:
            every("heatpump", 30.0, rng.uniform(0, 30), 2)
        if self.pv:
            every("pv", 60.0, rng.uniform(0, 60), 3)
        if self.battery:
            every("battery", 60.0, rng.uniform(0, 60), 4)
        if self.ev:
            ev = self.ev
            boot = rng.uniform(0.5, 8.0)  # drawn even when it falls outside a short run
            if boot < d:
                out.append(Sample(boot, "ev-status", 5))
            for at, _ in ev.changes[1:]:
                if at < d:
                    out.append(Sample(at, "ev-status", 5))
            for at in ev.session_starts():  # StartTransaction, just before the status turns Charging
                if at - 0.2 < d:
                    out.append(Sample(at - 0.2, "ev-start", 5))
            if ev.transaction_open(0.0):
                every("ev-meter", 60.0, rng.uniform(10, 60), 6)
            else:
                opened = next((at for at, st in ev.changes if st == "Charging"), None)
                if opened is not None:
                    every("ev-meter", 60.0, 60.0, 6, start=opened)
        out.sort(key=lambda s: (s.at, s.order))
        return out

    def run(self) -> Iterable[Event]:
        now = 0.0
        for s in self.schedule():
            self.advance(now, s.at)
            now = s.at
            yield from self.publish(s)

    def publish(self, s: Sample) -> Iterable[Event]:
        t, rng, home = s.at, self.rng, self.home
        unix = self.t0 + t
        topic = f"home/{home}/"
        if s.device == "p1":
            # the meter's clock ticks in whole seconds; the reader publishes a moment later
            net, *_ = self.powers(t)
            volts = self.voltages(t, net)
            yield Event(at=t + rng.uniform(0.15, 0.6), client=f"{home}-p1", topic=topic + "p1",
                        payload=self.meter.telegram(unix, net, volts))
        elif s.device == "thermostat":
            noise = 0.0 if self.hp and self.hp.scenario == "lockout" else self.room_swing(t)
            temp = round(self.room + noise + rng.gauss(0, 0.02), 1)
            sp = self.setpoint_at(t)
            self.humidity = min(max(self.humidity + rng.gauss(0, 0.4), 30), 55)
            # the same water vapour reads as a higher relative humidity in cooler air
            rh = self.humidity * vapour_pressure(self.room_at_start) / vapour_pressure(self.room)
            yield Event(at=t, client=f"{home}-thermostat", topic=topic + "thermostat", payload=jbytes({
                "ts": ts_ms(self.t0, t), "temp_c": temp, "setpoint_c": sp,
                "setpoint_since": int(self.setpoint_since(t)) * 1000, "humidity_pct": round(rh),
                "mode": self.mode, "heating": self.mode == "heat" and temp < sp - 0.1,
            }))
        elif s.device == "heatpump":
            st = self.hp.state(t)
            meter = 1 + rng.gauss(0, 0.006) if st["compressor"] else 1.0  # the controller's own metering
            yield Event(at=t, client=f"{home}-heatpump", topic=topic + "heatpump", payload=jbytes({
                "ts": ts_ms(self.t0, t), "mode": st["mode"], "compressor": st["compressor"],
                "compressor_hz": st["hz"], "elec_w": round(st["elec"] * meter), "heat_w": round(st["heat"]),
                "flow_c": round(st["flow"] + rng.gauss(0, 0.06), 1), "return_c": round(st["ret"] + rng.gauss(0, 0.06), 1),
                "flow_lpm": round(st["lpm"], 1),
                "outdoor_c": round(st["outdoor_c"] + rng.gauss(0, 0.05), 1), "defrost": st["defrost"],
                "fault": st["fault"],
                "fault_since": ts_ms(self.t0, st["fault_since"]) if st["fault"] else None,
            }))
        elif s.device == "pv":
            ac = self.pv.ac_w(t)
            yield Event(at=t, client=f"{home}-pv", topic=topic + "pv", payload=jbytes({
                "ts": ts_ms(self.t0, t), "ac_w": round(ac), "dc_w": round(ac / 0.965) if ac else 0,
                "today_kwh": round(self.pv.today_kwh, 2), "state": "MPPT" if ac else "Sleeping",
            }))
        elif s.device == "battery":
            _, _, _, _, batt_w = self.powers(t)
            b = self.battery
            yield Event(at=t, client=f"{home}-battery", topic=topic + "battery", payload=jbytes({
                "ts": ts_ms(self.t0, t), "soc_pct": round(b.soc, 1), "power_w": round(batt_w),
                "mode": b.mode(unix), "temp_c": round(b.temp_c + 0.4 * abs(batt_w) / b.max_w, 1),
            }))
        elif s.device in ("ev-status", "ev-start", "ev-meter"):
            ev = self.ev
            stamp = datetime.fromtimestamp(ts_ms(self.t0, t) / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            retain = False
            if s.device == "ev-status":
                body = {"action": "StatusNotification", "connectorId": 1, "status": ev.status(t),
                        "errorCode": "NoError", "timestamp": stamp}
                retain = True
            elif s.device == "ev-start":
                body = {"action": "StartTransaction", "connectorId": 1, "idTag": ev.id_tag,
                        "meterStart": round(ev.register_wh), "timestamp": stamp}
            else:
                sampled = [("Power.Active.Import", "W", ev.power_w(t)),
                           ("Energy.Active.Import.Register", "Wh", ev.register_wh)]
                body = {"action": "MeterValues", "connectorId": 1, "transactionId": ev.tx, "meterValue": [{
                    "timestamp": stamp,
                    "sampledValue": [{"value": str(round(v)), "context": "Sample.Periodic", "measurand": m, "unit": u}
                                     for m, u, v in sampled],
                }]}
            yield Event(at=t, client=f"{home}-evcharger", topic=topic + "evcharger", payload=jbytes(body),
                        qos=1, retain=retain)


# ---- the households ------------------------------------------------------------------------


def seconds_until_local(t0: float, hour: int) -> float:
    """Seconds from t0 until the next hour:00 on the Danish clock."""
    lt = local_time(t0)
    target = lt.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= lt:
        target += timedelta(days=1)
    return (target - lt).total_seconds() - (t0 - math.floor(t0))


def households(rng: random.Random, t0: float, duration: float) -> List[Home]:
    weather = Weather(rng, t0)
    # hh-131's charger pauses for the peak tariff, on the clock: at 17:00, or from the start
    to_peak = seconds_until_local(t0, 17)
    in_peak = 17 <= local_hour(t0) < 21
    smart = [(0.0, "SuspendedEVSE" if in_peak else "Charging")]
    if not in_peak:
        smart.append((to_peak + 0.4, "SuspendedEVSE"))

    def mk(home: str, **kw) -> Home:
        # each home draws from its own generator, so changing one home leaves the others alone
        return Home(random.Random(rng.getrandbits(64)), t0, duration, weather, home, **kw)

    radiators, floor = (28.0, 1.0), (24.0, 0.65)
    return [
        mk("hh-104", size=1.2, volts=(236.2, 235.1, 237.0), setpoint=21.5,
           hp=dict(nominal_w=8000, ua=230, room_c=21.5, curve=radiators, lpm=17.0, three_phase=True),
           pv=(6.2, 225.0, True), battery=(10.0, 5.0, 74.0),
           ev=EvCharger("peak-start", 4711, (0, 1, 2), 6184220.0, "04E1A35A7C6B80",
                        [(0.0, "Available"), (281.3, "Preparing"), (304.7, "Charging")])),
        mk("hh-117", size=0.9, volts=(233.4, 234.6, 232.9), setpoint=21.5, room_start=19.40,
           hp=dict(nominal_w=7000, ua=250, room_c=21.5, curve=radiators, lpm=15.0, three_phase=False,
                   scenario="lockout")),
        mk("hh-123", size=1.1, volts=(238.1, 237.4, 238.8), setpoint=21.0,
           hp=dict(nominal_w=8000, ua=200, room_c=21.0, curve=floor, lpm=18.0, three_phase=True,
                   scenario="icing", outdoor_bias=0.4)),
        mk("hh-131", size=1.0, volts=(235.0, 236.3, 234.4), setpoint=20.0, comfort_step=True,
           pv=(4.1, 185.0, True),
           ev=EvCharger("smart-pause", 3302, (0, 1, 2), 3927615.0, "04B7C2196A2C81", smart)),
        mk("hh-142", size=1.0, volts=(218.6, 216.4, 219.3), feeder_ohm=0.35, setpoint=21.0, sag=(290, math.inf, 5.0),
           hp=dict(nominal_w=9000, ua=240, room_c=21.0, curve=radiators, lpm=19.0, three_phase=True,
                   scenario="defrost", outdoor_bias=-0.9),
           pv=(8.4, 268.0, True)),
        mk("hh-158", size=1.0, volts=(234.2, 233.0, 235.1), setpoint=21.5,
           ev=EvCharger("phev-full", 918, (0,), 1402880.0, "0453D80E2F4A80",
                        [(0.0, "Charging"), (470.2, "SuspendedEV")])),
        mk("hh-163", size=0.5, occupied=False, volts=(239.6, 240.8, 239.9), setpoint=16.0, mode="away",
           room_bias=0.3,
           hp=dict(nominal_w=6000, ua=150, room_c=16.0, curve=floor, lpm=13.0, three_phase=False),
           pv=(5.0, 180.0, True), battery=(7.0, 3.5, 56.0)),
        mk("hh-170", size=0.6, volts=(231.5, 232.2, 230.8), setpoint=22.0, room_bias=0.2),
        mk("hh-186", size=1.0, p1=False, setpoint=21.0,
           hp=dict(nominal_w=7000, ua=210, room_c=21.0, curve=floor, lpm=16.0, three_phase=False, scenario="dhw"),
           ev=EvCharger("idle", 0, (0, 1, 2), 2871304.0, "04297F1BC35E80", [(0.0, "Available")])),
        mk("hh-191", size=1.0, p1=False, setpoint=20.5, pv=(3.0, 200.0, False)),
        mk("hh-205", size=1.0, p1=False, setpoint=21.0, room_bias=-0.2),
        mk("hh-212", size=0.6, p1=False, setpoint=22.5),
    ]


def events(rng: random.Random, t0: float, duration: float) -> Iterable[Event]:
    for home in households(rng, t0, duration):
        yield from home.run()
