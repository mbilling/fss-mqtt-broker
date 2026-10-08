"""Power plants: a small Danish generation portfolio in DK2 (the Nordic synchronous area).

Four sites, each publishing what its real-world counterpart sends, in that device's own
format. Metric units, a 50 Hz grid. Every timestamp is the device's own clock, taken from
the simulation (`ts_ms(t0, at)`), and every random draw comes from the module's seeded
generator, so a seed, a start and a duration give the same bytes on every run.

Internally the physics runs at 1 s resolution, starting 180 s before the first message so
the minute averages and the filters are warm; only messages at 0 <= at < duration are
published.

The outside air follows the date and the hour (`_ambient`): a Danish climate, about 1 degC
on average in late January and 16.5 in late July, with a daily swing of 4 (winter) to 8 degC
(summer) that peaks at 15:00 solar time. On the fixture's late-March afternoon it is
6.4 degC and slowly falling.

wf-falster: a near-shore wind farm, 6 x 3.6 MW turbines (120 m rotor, 90 m hub), 21.6 MW
-------------------------------------------------------------------------------------------
Westerly wind of about 9.4 m/s at hub height: a slowly drifting farm mean, gusts shared by
the whole farm (shifted by a few seconds per turbine) and each turbine's own turbulence,
about 9 % turbulence intensity in all. Each turbine follows a 3.6 MW / 120 m power curve
(cut-in 3 m/s, rated 3,600 kW from about 12.5 m/s, cut-out 25 m/s), scaled below rated by
the air density at the outside temperature: 1.266 kg/m3 in the fixture's 6 degC air, 3.3 %
above the curve's 1.225; about 1.20 on a 20 degC summer afternoon. With rotor inertia,
variable rotor speed (5-13 rpm, tip-speed ratio 8.2) and pitch.

  plant/wf-falster/wtg01..wtg06/tele    every 10 s per turbine, QoS 0 (360 messages / 600 s)
    IEC 61400-25-2 flavoured JSON: the logical nodes as objects, their data objects inside.
      {"ts": ms,
       "WTUR": {"TurSt": {"stVal": state, "t": ms when stVal last changed},
                "AlmCd": vendor alarm code (0 none, 2310 gearbox oil hot - power limited,
                         2311 gearbox oil high-high - stopped,
                         3420 drivetrain vibration high-high - stopped),
                "W": active power kW, "VAr": reactive power kvar},
       "WROT": {"RotSpd": rotor rpm, "PtchAng": blade pitch deg},
       "WNAC": {"WdSpd": nacelle wind speed m/s, "Dir": wind direction deg,
                "ExTmp": outside air degC, "NacTmp": inside the nacelle degC},
       "WTRM": {"TmpGbxOil": gearbox oil sump degC,
                "VibGbx": gearbox vibration velocity, mm/s RMS, 10-1000 Hz},
       "WGEN": {"Spd": generator rpm, "TmpStat": stator winding degC},
       "WYAW": {"YwAng": nacelle position deg}}
    Power, wind, speeds, pitch and vibration are 10-s means; temperatures are the value
    now. TurSt codes: 1 ready (below cut-in), 2 starting, 3 producing, 4 derated (limited
    by the turbine's own protection), 5 stopped (remote/manual), 6 fault (tripped),
    7 maintenance. `t` is the time of the last change of stVal, which every IEC 61850
    status value carries. A stopped turbine draws about 9 kW for its auxiliaries, so its
    W is slightly negative.

  plant/wf-falster/ppc/stats            every 60 s, QoS 1 (10 messages / 600 s)
    The park power controller's 1-minute batch, means over [ts - 60 s, ts):
      {"ts": end of the minute, "site", "period_s": 60, "P_kW": farm total, "Q_kvar",
       "WdSpd": farm mean, "running": turbines in state 3 or 4,
       "turbines": [{"id", "TurSt": code at ts, "WdSpd", "W", "RotSpd",
                     "TmpGbxOil", "TmpStat", "VibGbx"}, ...]}

  plant/wf-falster/poc/grid             every 2 s, QoS 0 (300 messages / 600 s)
    The phasor measurement unit (PMU, IEC/IEEE 60255-118-1) at the farm's 50 kV point of
    connection, the frequency metering an FCR provider installs, as the site gateway
    forwards one of its frames every 2 s (client wf-falster-pmu):
      {"ts", "Hz": system frequency (1 mHz resolution), "ROCOF": Hz/s over the last 0.5 s,
       "U_kV": {"L12", "L23", "L31"} line-line kV, "P_MW", "Q_Mvar": the farm's export}
    (A power-quality meter would not do: IEC 61000-4-30 Class A reports frequency as a 10-s
    value and no ROCOF.) Nordic frequency: 50 Hz with slow FCR-N-scale wander, inside
    49.93-50.06 Hz.

pv-lolland: a solar park, 4 central inverters of 2,200 kVA, each 2.75 MWp (11 MWp)
-----------------------------------------------------------------------------------
Fixed-tilt modules (30 deg, facing south) at 54.8 N 11.6 E. The irradiance on the panels
comes from the sun's real position at the simulated time (clear sky, thin high cloud),
so on the fixture's late-March afternoon it is low and falling (about 200 to 175 W/m2,
the sun 13.4 to 12.0 deg up in the west-south-west, setting at about 18:30 local) and
each inverter delivers about 550 falling to 440 kW; at night the inverters sleep.

  plant/pv-lolland/inv01..inv04/sunspec  every 30 s per inverter, QoS 0 (80 messages / 600 s)
    SunSpec model 103 (three-phase inverter) points as a Modbus-to-MQTT gateway reads
    them: the raw register integers and their scale factors, value = register x 10^SF.
      A/A_SF            AC current, A                 (SF -1)
      PPVphAB/BC/CA, V_SF  AC line-line voltage, V   (SF -1)
      W/W_SF            AC active power, W            (SF 2: 100 W steps)
      Hz/Hz_SF          grid frequency, Hz            (SF -2)
      VA/VA_SF, VAr/VAr_SF  apparent / reactive power (SF 2)
      PF/PF_SF          power factor, percent         (SF -2); -32768 when not producing
      WH/WH_SF          lifetime AC energy, Wh        (SF 3: kWh steps)
      DCA/DCA_SF, DCV/DCV_SF, DCW/DCW_SF  DC current A, voltage V, power W  (SF -1, -1, 2)
      TmpCab, TmpSnk, TmpTrns, TmpOt with Tmp_SF  degC (SF -1); -32768 = not implemented
      St                1 OFF, 2 SLEEPING, 3 STARTING, 4 MPPT, 5 THROTTLED,
                        6 SHUTTING_DOWN, 7 FAULT, 8 STANDBY
      StVnd             the vendor's sub-state (5010 ramp limit, 5020 limited at nameplate)
      Evt1              bitfield: 0 GROUND_FAULT, 1 DC_OVER_VOLT, 2 AC_DISCONNECT,
                        3 DC_DISCONNECT, 4 GRID_DISCONNECT, 5 CABINET_OPEN,
                        6 MANUAL_SHUTDOWN, 7 OVER_TEMP, 8 OVER_FREQUENCY,
                        9 UNDER_FREQUENCY, 10 AC_OVER_VOLT, 11 AC_UNDER_VOLT,
                        12 BLOWN_STRING_FUSE, 13 UNDER_TEMP, 14 MEMORY_LOSS,
                        15 HW_TEST_FAILURE
      Evt2, EvtVnd1     reserved / the vendor's event bits (4096 = residual-current trip)
    The raw numbers are meaningless until scaled: W 5094 with W_SF 2 is 509.4 kW.
    The heat sink and the cabinet follow the losses with a lag (time constants of 5 and
    15 minutes), so a tripped inverter cools over minutes, not at once. With a DC/AC ratio
    of 1.25 an inverter clips at its 2,200 kVA nameplate around a clear summer noon: it then
    reports St 5 (THROTTLED, StVnd 5020) and moves its array off the MPP towards Voc.

pk-koge: a 45 MW open-cycle gas turbine peaker (10.5 kV generator) behind a legacy RTU
-------------------------------------------------------------------------------------
  plant/pk-koge/g1/rtu                  every 15 s, QoS 0 (40 messages / 600 s)
    One CSV line, no header, as a serial-to-MQTT gateway forwards the RTU's poll:
      ts,unit,MW,MVAr,kV,Hz,breaker
      ts       Unix seconds (RTU clock, UTC)
      unit     unit name (G1)
      MW       active power, 1 decimal
      MVAr     reactive power, 1 decimal; EMPTY while the transducer reads invalid
               (machine de-energized)
      kV       generator terminal voltage, line-line, 2 decimals
      Hz       generator frequency, 3 decimals; EMPTY while de-energized
      breaker  generator breaker as an IEC 60870-5 double point:
               0 intermediate, 1 open, 2 closed, 3 faulty
    e.g. "1774367702,G1,0.0,,0.00,,1" (standby) and "1774368302,G1,17.9,3.6,10.55,49.994,2".

The fixture (--seed 7 --start 2026-03-24T15:55:00Z --duration 600: 16:55-17:05 CET)
-------------------------------------------------------------------------------------
Normal data stays well inside every limit the rules alert on: gearbox oil 55-63 degC,
gearbox vibration 1.1-2.5 mm/s (10-s means), frequency 49.94-50.02 Hz (the model keeps it
within 49.93-50.06), healthy turbines at 0.99-1.06 of their power curve. Injected, in
seconds after the start (the schedule is the same for any seed; the noise around it, and
so the exact values, is not):

  5     WTG03's gearbox oil cooler fan fails. The sump (this older gearbox runs at about
        62 degC) climbs about 2.2 degC/min: its 1-minute mean is 74.3 degC for the minute
        to 16:00 UTC, 76.5 to 16:01 (warning), 80.7 to 16:03 (critical). At 82 degC (1-s
        value, 496 s, 17:03:16 local) the turbine's own protection derates it to 1,200 kW,
        a third of rated (TurSt 4, AlmCd 2310): at this wind it gives up about 40 % of
        its power, and the oil's rise slows from about 2 to under 1 degC/min (83.6 degC
        at 594 s). It would stop at 90 degC (AlmCd 2311), which a 600-s run does not reach.
        The timescale is compressed for the demo: with several tonnes of steel to heat, a
        real gearbox takes 30-60 minutes to reach its alarm level after its cooling fails.
  70    The peaker G1 starts for the 17:00 evening peak: de-energized until 225 s, at full
        speed and excited 225-290 s, breaker closed at 290 s (16:59:50 local), then
        ramping at 8 MW/min to about 40 MW.
  96    INV03 trips on GROUND_FAULT: its residual-current monitor (RCMU, IEC 62109-2) sees a
        sudden jump in the array's leakage current, say through one cracked string
        connector as the evening turns damp. St 7, Evt1 bit 0, EvtVnd1 4096, AC power 0,
        the array at open-circuit voltage. The insulation-resistance (Riso) test it runs
        before reconnecting passes at 180 s (the leak is intermittent), so it reconnects by
        itself: it waits 60 s (STANDBY, the EN 50549-1 reconnection observation time),
        restarts at 240 s, ramps back at 10 % of rated power per minute (THROTTLED,
        259-379 s) and is in MPPT again from 409 s. A fault like this comes back night
        after night until a technician finds the connector, which is why the alert matters.
  125   WTG05's pitch control comes back from an encoder reset 4 deg off its optimum
        (PtchAng 4.5 against the others' 0.5): the rotor converts about a quarter less of
        the wind's power while the turbine still reports TurSt 3 (producing). Only a
        comparison with the power curve shows it: 0.75-0.79 of expected, every minute.
  150   WTG06's gearbox is damaged (say a cracked gear tooth, or a component working
        loose) and its drivetrain vibration climbs: its 1-minute mean is 3.89 mm/s for
        the minute to 15:59 (ISO 10816-21 zone C), 4.88 to 16:00, 5.81 to 16:01 (zone D).
        The timescale is compressed for the demo: a developing bearing or gear defect takes
        weeks to months, and the turbine's condition-monitoring system (CMS) would raise an
        alarm long before. What stops it here is the controller's high-high stop on the
        CMS's gearbox velocity, which some OEMs configure (others leave the stop to the
        operator; the safety chain's own vibration switch watches low-frequency nacelle
        acceleration, not the gearbox): at a 10-s mean of 7.1 mm/s (372 s, 17:01:11 local)
        it stops, TurSt 6, AlmCd 3420; then the rotor idles and the turbine draws 9 kW for
        its auxiliaries.
  403   A large generator elsewhere in the Nordic system trips (17:01:43 local). The
        frequency falls at up to 0.064 Hz/s to a nadir of 49.75 Hz 8 s later. FCR-N and
        FCR-D, both proportional, stop the fall and hold it at a quasi-steady 49.85-49.89 Hz;
        from about a minute after the trip aFRR brings it back with a time constant of about
        two minutes. 72 of the PMU's 80 readings from 405 to 563 s are below 49.9 Hz (the
        last ones within its noise of 49.90), and it is back at 49.92 by 600 s. The PMU,
        the inverters' Hz and the RTU's Hz all show the same dip.
  437   One RTU line arrives truncated ("1774368137,G1,19.5,4.") - a serial glitch.
"""

from __future__ import annotations

import math
import random
from typing import Dict, Iterable, List, Optional, Tuple

from .core import Event, jbytes, ts_ms

WARMUP = 180  # seconds of physics simulated before the first message

WF, PV, PK = "wf-falster", "pv-lolland", "pk-koge"
LAT, LON = 54.77, 11.62  # Lolland-Falster

# The turbine's power curve at rho = 1.225 kg/m3: (wind m/s, kW), linear between points.
CURVE = [(3, 25), (4, 165), (5, 360), (6, 640), (7, 1020), (8, 1520), (9, 2120),
         (10, 2760), (11, 3260), (12, 3530), (13, 3600)]
RATED_KW = 3600.0
CURVE_RHO = 1.225  # kg/m3, the curve's reference density (IEC 61400-12-1)

# Gearbox oil, WTG03: its cooler fan fails at OIL_FAULT_AT; from then on
# dT/dt = (T_eq - T) / tau with the no-cooling equilibrium T_eq = 70 + 150 * P/Prated.
OIL_FAULT_AT, OIL_FAULT_TAU = 5.0, 2400.0
OIL_DERATE_C, OIL_DERATE_KW = 82.0, 1200.0  # a third of rated: it binds at any useful wind
OIL_TRIP_C = 90.0  # high-high: the turbine stops (not reached in a 600 s run)
VIB_TRIP = 7.1  # mm/s, 10-s mean: the controller's high-high stop on the CMS gearbox velocity
# WTG06 gearbox damage: extra vibration (mm/s) at these times (s), linear in between.
# Compressed into minutes for the demo (see the module doc).
VIB_DEFECT = [(150, 0.0), (190, 1.7), (240, 2.6), (300, 3.6), (355, 4.6), (380, 6.0)]
# WTG05: after a pitch-encoder reset its blades sit 4 deg off the optimum, which costs
# about a quarter of the power below rated wind; the turbine still reports "producing".
PITCH_FAULT_AT, PITCH_OFFSET_DEG, PITCH_KEEP = 125.0, 4.0, 0.74

INV_FAULT_AT, INV_RETEST_AT, INV_RESTART_AT, INV_RAMP_AT = 96.3, 180.0, 240.0, 252.0
INV_RATED_KVA = 2200.0
INV_RAMP_KW_PER_S = INV_RATED_KVA * 0.10 / 60.0  # 10 % of rated per minute
INV_TAU_SINK_S, INV_TAU_CAB_S = 300.0, 900.0  # thermal time constants

FREQ_EVENT_AT = 403.0
# The trip's frequency response (see _Grid._dip): the inertial dip, the quasi-steady
# deviation the proportional FCR leaves, and aFRR restoring it after a minute.
FREQ_FAST_HZ, FREQ_HELD_HZ = 0.15, 0.12
FREQ_AFRR_AFTER, FREQ_AFRR_TAU = 60.0, 120.0

# The start command at 70 s is invisible to the RTU, which only sees the generator side.
PEAKER_EXCITED, PEAKER_SYNC = 225.0, 290.4
PEAKER_RAMP_MW_PER_S, PEAKER_TARGET_MW = 8.0 / 60.0, 40.0
RTU_GLITCH_AT = 437.0


def events(rng: random.Random, t0: float, duration: float) -> Iterable[Event]:
    """The portfolio's messages for `duration` seconds from `t0` (Unix seconds)."""
    # One generator per subsystem, seeded from `rng` in a fixed order, and inside each one
    # generator per process, drawn second by second: the sites stay independent of each
    # other's draws, and the first N seconds are the same whatever the duration.
    seeds = {name: rng.getrandbits(64) for name in ("grid", "wind", "solar", "peaker")}
    n = max(0, int(math.ceil(duration)))
    grid = _Grid(random.Random(seeds["grid"]), n)
    farm = _WindFarm(random.Random(seeds["wind"]), n, t0)
    out: List[Event] = []
    out += farm.events(t0, duration)
    out += grid.events(t0, duration, farm)
    out += _solar_events(random.Random(seeds["solar"]), t0, duration, grid)
    out += _peaker_events(random.Random(seeds["peaker"]), t0, duration, grid)
    return out


# --------------------------------------------------------------------------- helpers


def _r(x: float, nd: int) -> float:
    """Round for publishing, without a negative zero."""
    v = round(x, nd)
    return v if v != 0 else 0.0


class _OU:
    """An Ornstein-Uhlenbeck process: mean-reverting noise with time constant `tau` (s)
    and stationary standard deviation `sigma`, stepped once per second."""

    def __init__(self, rng: random.Random, tau: float, sigma: float, x0: Optional[float] = None):
        self.rng, self.tau, self.sigma = rng, tau, sigma
        self.x = rng.gauss(0.0, sigma) if x0 is None else x0
        self.k = math.sqrt(2.0 / tau) * sigma

    def step(self) -> float:
        self.x += -self.x / self.tau + self.k * self.rng.gauss(0.0, 1.0)
        return self.x


def _interp(points: List[Tuple[float, float]], x: float) -> float:
    if x <= points[0][0]:
        return points[0][1]
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if x <= x1:
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return points[-1][1]


def _ambient(unix: float) -> float:
    """Outside air, degC, at Unix time `unix`: a Danish climate without weather.

    The daily mean runs from about 1 degC (late January) to 16.5 (late July), and half the
    daily range from 2 to 4 degC; the day peaks at 15:00 solar time. 6.4 degC at 16:55 CET
    on 24 March, the fixture's start."""
    d = unix / 86400.0 - 10957.5  # days since J2000.0 (1 January 2000, 12:00 UTC)
    season = math.cos(2.0 * math.pi * ((d + 1.5) % 365.2422 - 30.0) / 365.2422)
    solar_h = (unix / 3600.0 + LON / 15.0) % 24.0
    return 8.8 - 7.7 * season + (3.0 - 1.0 * season) * math.cos(2.0 * math.pi * (solar_h - 15.0) / 24.0)


def _density_gain(t_air: float) -> float:
    """Air density at `t_air` degC relative to the curve's 1.225 kg/m3 (1.266 at 6.4 degC)."""
    return 1.266 * (273.15 + 6.4) / (273.15 + t_air) / CURVE_RHO


def _curve_kw(v: float, gain: float) -> float:
    """Available power at hub wind speed `v`, the curve scaled by the air density `gain`."""
    if v < 3.0 or v >= 25.0:
        return 0.0
    if v >= 13.0:
        return RATED_KW
    return min(RATED_KW, _interp(CURVE, v) * gain)


def _at_list(offset: float, period: float, duration: float) -> List[float]:
    out, k = [], 0
    while offset + k * period < duration:
        out.append(round(offset + k * period, 3))
        k += 1
    return out


# --------------------------------------------------------------------------- grid


class _Grid:
    """System frequency (Nordic area) and the 50 kV voltage at the wind farm's POC."""

    def __init__(self, rng: random.Random, n: int):
        series = random.Random(rng.getrandbits(64))
        self.rng = random.Random(rng.getrandbits(64))  # the meter's own noise
        wander = _OU(series, 45.0, 0.022)
        volt = _OU(series, 400.0, 0.10)
        self.f_base: List[float] = []
        self.u_base: List[float] = []
        for _ in range(-WARMUP, n + 2):
            # FCR-N keeps the normal wander small: soft-limited to +-65 mHz.
            self.f_base.append(49.992 + 0.062 * math.tanh(wander.step() / 0.062))
            self.u_base.append(51.35 + volt.step())

    @staticmethod
    def _dip(tau: float) -> float:
        """Frequency drop (Hz) `tau` s after the generator trip.

        System inertia and the fast FCR-D response give a nadir of about 0.25 Hz 7-8 s
        after the trip. FCR-N and FCR-D are proportional (droop) reserves: they stop the
        fall and hold it, they do not remove the deviation, so the frequency settles at a
        quasi-steady 0.12 Hz below where it was. aFRR, activated by the TSOs' load-frequency
        controller about a minute later, restores it with a time constant of about two
        minutes (mFRR would take over a lasting deficit)."""
        if tau <= 0:
            return 0.0
        x = tau / 6.5
        fast = FREQ_FAST_HZ * x ** 1.5 * math.exp(1.5 * (1.0 - x))
        held = FREQ_HELD_HZ * (1.0 - math.exp(-tau / 4.0))
        afrr = math.exp(-max(0.0, tau - FREQ_AFRR_AFTER) / FREQ_AFRR_TAU)
        return fast + held * afrr

    def f(self, t: float) -> float:
        """Frequency at `t` s after the start (linear between seconds)."""
        i = t + WARMUP
        lo = max(0, min(len(self.f_base) - 2, int(math.floor(i))))
        frac = min(1.0, max(0.0, i - lo))
        base = self.f_base[lo] * (1 - frac) + self.f_base[lo + 1] * frac
        return base - self._dip(t - FREQ_EVENT_AT)

    def u(self, t: float) -> float:
        i = max(0, min(len(self.u_base) - 1, int(t) + WARMUP))
        x = (t - FREQ_EVENT_AT) / 6.5
        sag = 0.12 * x ** 1.5 * math.exp(1.5 * (1.0 - x)) if x > 0 else 0.0
        return self.u_base[i] - sag

    def events(self, t0: float, duration: float, farm: "_WindFarm") -> List[Event]:
        out = []
        phase = (0.03, -0.02, 0.05)
        for at in _at_list(1.0, 2.0, duration):
            f = self.f(at) + self.rng.gauss(0.0, 0.0008)
            rocof = (self.f(at) - self.f(at - 0.5)) / 0.5 + self.rng.gauss(0.0, 0.002)
            u = self.u(at)
            p_mw = farm.p_total(at) * 0.986 / 1000.0  # 1.4 % collection-grid losses
            out.append(Event(at=at, client=f"{WF}-pmu", topic=f"plant/{WF}/poc/grid", payload=jbytes({
                "ts": ts_ms(t0, at),
                "Hz": _r(f, 3),
                "ROCOF": _r(rocof, 3),
                "U_kV": {k: _r(u + d + self.rng.gauss(0.0, 0.01), 2)
                         for k, d in zip(("L12", "L23", "L31"), phase)},
                "P_MW": _r(p_mw, 2),
                "Q_Mvar": _r(self.rng.gauss(-0.05, 0.06), 2),
            })))
        return out


# --------------------------------------------------------------------------- wind


class _Turbine:
    def __init__(self, idx: int, rng: random.Random):
        self.rng = rng  # this turbine's own draws, second by second
        self.id = f"wtg{idx:02d}"
        self.site = rng.uniform(0.975, 1.025)  # exposure: hill, shore distance
        self.eff = rng.uniform(0.975, 1.015)  # blade condition, calibration
        self.lag = rng.randint(0, 4)  # gusts reach it this many seconds later
        self.offset = (0.7, 2.3, 3.9, 5.1, 6.6, 8.2)[idx - 1]  # its 10-s publish phase
        self.oil_off = rng.uniform(-2.5, 2.5)
        if self.id == "wtg03":
            self.oil_off = 5.1  # an older gearbox that runs warm
        self.gen_off = rng.uniform(-3.0, 3.0)
        self.nac_off = rng.uniform(-1.5, 1.5)
        self.vib_gain = rng.uniform(0.9, 1.1)
        self.turb = _OU(rng, 5.0, 0.6)
        self.since_s = -float(rng.randint(4 * 3600, 70 * 3600))  # producing since (s, < 0)


class _WindFarm:
    def __init__(self, rng: random.Random, n: int, t0: float):
        self.n, self.t0 = n, t0
        self.turbines = [_Turbine(i, random.Random(rng.getrandbits(64))) for i in range(1, 7)]
        mean, gust, wdir = (_OU(random.Random(rng.getrandbits(64)), tau, sigma)
                            for tau, sigma in ((300.0, 0.45), (20.0, 0.55), (150.0, 5.0)))
        span = WARMUP + n + 12
        self.gust = [gust.step() for _ in range(span + 8)]
        self.mean = [9.4 + mean.step() for _ in range(span)]
        self.dir = [251.0 + wdir.step() for _ in range(span)]
        self.series = [self._run(t) for t in self.turbines]

    def _run(self, t: _Turbine) -> Dict[str, list]:
        """One turbine, second by second, from -WARMUP."""
        rng = t.rng
        keys = ("ws", "w", "var", "rpm", "pitch", "dir", "yaw", "oil", "gen", "nac", "ext", "vib", "st")
        s: Dict[str, list] = {k: [] for k in keys}
        v_eff = self.mean[0]
        rpm = 12.0
        pitch = 0.5
        yaw = self.dir[0]
        p_frac0 = _curve_kw(v_eff, _density_gain(_ambient(self.t0 - WARMUP))) / RATED_KW
        oil = 49.0 + 14.0 * p_frac0 + t.oil_off
        gen = 55.0 + 30.0 * p_frac0 ** 1.3 + t.gen_off
        state, state_t, alarm = 3, t.since_s, 0
        limit = RATED_KW
        vib_window: List[float] = []
        dir_window: List[float] = []
        for i, sec in enumerate(range(-WARMUP, self.n + 12)):
            g = self.gust[i + 8 - t.lag]
            v = max(0.0, (self.mean[i] + g + t.turb.step()) * t.site)
            v_eff += (v - v_eff) / 4.0  # rotor inertia
            t_air = _ambient(self.t0 + sec)
            avail = _curve_kw(v_eff, _density_gain(t_air)) * t.eff
            pitch_error = 0.0
            if t.id == "wtg05" and sec >= PITCH_FAULT_AT:
                avail *= PITCH_KEEP
                pitch_error = PITCH_OFFSET_DEG
            if t.id == "wtg03" and state == 3 and sec >= 0 and oil >= OIL_DERATE_C:
                # (a change found in second `sec` happened during (sec - 1, sec])
                state, state_t, alarm, limit = 4, sec - 0.63, 2310, OIL_DERATE_KW
            if t.id == "wtg03" and state == 4 and oil >= OIL_TRIP_C:
                state, state_t, alarm = 6, sec - 0.41, 2311
            if state == 6:
                p = -9.0 + rng.gauss(0.0, 0.4)  # auxiliaries
                rpm += (0.6 - rpm) / 25.0
                pitch = min(88.0, pitch + 6.0)
            else:
                p = min(avail, limit) * (1.0 + rng.gauss(0.0, 0.006))
                target_rpm = min(13.0, max(5.0, 1.305 * v_eff)) if avail > 0 else 1.0
                rpm += (target_rpm - rpm) / 6.0
                want_pitch = 0.5 + pitch_error
                if avail > limit:
                    want_pitch += 14.0 * (1.0 - limit / avail)
                if v_eff > 12.5:
                    want_pitch += 2.2 * (v_eff - 12.5)
                pitch += (want_pitch - pitch) / 5.0
            p_frac = max(0.0, p) / RATED_KW
            # gearbox oil
            if t.id == "wtg03" and sec >= OIL_FAULT_AT:
                oil += (70.0 + 150.0 * p_frac - oil) / OIL_FAULT_TAU
            else:
                oil += (49.0 + 14.0 * p_frac + t.oil_off - oil) / 900.0
            gen += (55.0 + 30.0 * p_frac ** 1.3 + t.gen_off - gen) / 600.0
            # vibration velocity, 1-s RMS
            if state == 6:
                vib = 0.2 * math.exp(rng.gauss(0.0, 0.1))
            else:
                vib = (0.55 + 2.0 * p_frac) * t.vib_gain * math.exp(rng.gauss(0.0, 0.10))
                if t.id == "wtg06":
                    vib += _interp(VIB_DEFECT, sec) * math.exp(rng.gauss(0.0, 0.05)) if sec > 150 else 0.0
            vib_window = (vib_window + [vib])[-10:]
            if t.id == "wtg06" and state == 3 and sec >= 0 and sum(vib_window) / len(vib_window) >= VIB_TRIP:
                state, state_t, alarm = 6, sec - 0.38, 3420
            # yaw: follow the 30-s mean direction when the error exceeds 6 degrees
            d = self.dir[i] + rng.gauss(0.0, 1.5)
            dir_window = (dir_window + [d])[-30:]
            err = sum(dir_window) / len(dir_window) - yaw
            if abs(err) > 6.0 and state != 6:
                yaw += max(-0.5, min(0.5, err))
            s["ws"].append(v)
            s["w"].append(p)
            s["var"].append(-0.012 * max(0.0, p) + rng.gauss(0.0, 3.0))
            s["rpm"].append(rpm)
            s["pitch"].append(pitch)
            s["dir"].append(d)
            s["yaw"].append(yaw)
            s["oil"].append(oil + rng.gauss(0.0, 0.08))
            s["gen"].append(gen + rng.gauss(0.0, 0.1))
            # the nacelle is heated to about 20 degC; on a warm day it runs above the air
            s["nac"].append(20.5 + t.nac_off + 2.0 * p_frac + 0.8 * max(0.0, t_air - 10.0)
                            + rng.gauss(0.0, 0.05))
            s["ext"].append(t_air + rng.gauss(0.0, 0.05))
            s["vib"].append(vib)
            s["st"].append((state, state_t, alarm))
        return s

    @staticmethod
    def _mean(xs: list, lo: int, hi: int) -> float:
        part = xs[lo:hi]
        return sum(part) / len(part)

    def p_total(self, at: float) -> float:
        i = int(at) + WARMUP
        return sum(s["w"][i] for s in self.series)

    def events(self, t0: float, duration: float) -> List[Event]:
        out = []
        client = f"{WF}-scada"
        for t, s in zip(self.turbines, self.series):
            for at in _at_list(t.offset, 10.0, duration):
                hi = int(math.floor(at)) + WARMUP + 1  # the 10 s up to and including now
                lo = hi - 10
                m = {k: self._mean(s[k], lo, hi) for k in ("w", "var", "rpm", "pitch", "ws", "dir", "vib")}
                now = hi - 1
                state, state_t, alarm = s["st"][now]
                out.append(Event(at=at, client=client, topic=f"plant/{WF}/{t.id}/tele", payload=jbytes({
                    "ts": ts_ms(t0, at),
                    "WTUR": {"TurSt": {"stVal": state, "t": ts_ms(t0, state_t)},
                             "AlmCd": alarm, "W": _r(m["w"], 1), "VAr": _r(m["var"], 1)},
                    "WROT": {"RotSpd": _r(m["rpm"], 2), "PtchAng": _r(m["pitch"], 1)},
                    "WNAC": {"WdSpd": _r(m["ws"], 2), "Dir": _r(m["dir"] % 360.0, 1),
                             "ExTmp": _r(s["ext"][now], 1), "NacTmp": _r(s["nac"][now], 1)},
                    "WTRM": {"TmpGbxOil": _r(s["oil"][now], 1), "VibGbx": _r(m["vib"], 2)},
                    "WGEN": {"Spd": int(round(m["rpm"] * 119.0)), "TmpStat": _r(s["gen"][now], 1)},
                    "WYAW": {"YwAng": _r(s["yaw"][now] % 360.0, 1)},
                })))
        for at in _at_list(0.0, 60.0, duration):
            hi = int(at) + WARMUP
            lo = hi - 60
            rows = []
            for t, s in zip(self.turbines, self.series):
                rows.append({
                    "id": t.id,
                    "TurSt": s["st"][hi][0],
                    "WdSpd": _r(self._mean(s["ws"], lo, hi), 2),
                    "W": _r(self._mean(s["w"], lo, hi), 1),
                    "RotSpd": _r(self._mean(s["rpm"], lo, hi), 2),
                    "TmpGbxOil": _r(self._mean(s["oil"], lo, hi), 1),
                    "TmpStat": _r(self._mean(s["gen"], lo, hi), 1),
                    "VibGbx": _r(self._mean(s["vib"], lo, hi), 2),
                })
            out.append(Event(at=at, client=f"{WF}-ppc", topic=f"plant/{WF}/ppc/stats", qos=1, payload=jbytes({
                "ts": ts_ms(t0, at),
                "site": WF,
                "period_s": 60,
                "P_kW": _r(sum(r["W"] for r in rows), 1),
                "Q_kvar": _r(sum(self._mean(s["var"], lo, hi) for s in self.series), 1),
                "WdSpd": _r(sum(r["WdSpd"] for r in rows) / len(rows), 2),
                "running": sum(1 for r in rows if r["TurSt"] in (3, 4)),
                "turbines": rows,
            })))
        return out


# --------------------------------------------------------------------------- solar


def _sun(unix: float) -> Tuple[float, float]:
    """Solar elevation and azimuth (degrees; azimuth clockwise from north) at LAT/LON."""
    d = unix / 86400.0 - 10957.5  # days since J2000.0
    g = math.radians((357.529 + 0.98560028 * d) % 360.0)
    q = (280.459 + 0.98564736 * d) % 360.0
    lam = math.radians(q + 1.915 * math.sin(g) + 0.020 * math.sin(2 * g))
    eps = math.radians(23.439 - 0.00000036 * d)
    ra = math.atan2(math.cos(eps) * math.sin(lam), math.cos(lam))
    dec = math.asin(math.sin(eps) * math.sin(lam))
    gmst_h = (18.697374558 + 24.06570982441908 * d) % 24.0
    ha = math.radians(gmst_h * 15.0 + LON) - ra
    lat = math.radians(LAT)
    el = math.asin(math.sin(lat) * math.sin(dec) + math.cos(lat) * math.cos(dec) * math.cos(ha))
    az = math.atan2(math.sin(ha), math.cos(ha) * math.sin(lat) - math.tan(dec) * math.cos(lat))
    return math.degrees(el), (math.degrees(az) + 180.0) % 360.0


def _poa(unix: float, tilt: float = 30.0, facing: float = 180.0) -> float:
    """Clear-sky irradiance on the tilted panels, W/m2 (Haurwitz GHI, Meinel DNI)."""
    el, az = _sun(unix)
    if el <= 0.5:
        return 0.0
    sel = math.sin(math.radians(el))
    ghi = 1098.0 * sel * math.exp(-0.057 / sel)
    am = 1.0 / (sel + 0.50572 * (el + 6.07995) ** -1.6364)
    dni = 1361.0 * 0.7 ** (am ** 0.678)
    dhi = max(0.0, ghi - dni * sel)
    tr, fr = math.radians(tilt), math.radians(facing)
    cos_aoi = (sel * math.cos(tr)
               + math.cos(math.radians(el)) * math.sin(tr) * math.cos(math.radians(az) - fr))
    return max(0.0, dni * cos_aoi) + dhi * (1 + math.cos(tr)) / 2 + ghi * 0.2 * (1 - math.cos(tr)) / 2


def _inverter_ac_kw(p_dc: float, rated_kw: float = INV_RATED_KVA) -> float:
    """AC output of a central inverter: tare, linear and resistive losses (98.4 % peak)."""
    return max(0.0, p_dc - (0.003 * rated_kw + 0.009 * p_dc + 0.007 * p_dc * p_dc / rated_kw))


def _inverter_dc_kw(p_ac: float, rated_kw: float = INV_RATED_KVA) -> float:
    """The DC input that gives `p_ac` (the inverse of `_inverter_ac_kw`)."""
    a, b, c = 0.007 / rated_kw, -(1.0 - 0.009), 0.003 * rated_kw + p_ac
    return (-b - math.sqrt(b * b - 4.0 * a * c)) / (2.0 * a)


def _off_mpp_v(v_mp: float, v_oc: float, p_dc: float, p_dc_mpp: float) -> float:
    """DC voltage of an array held at `p_dc` below its MPP power, on the Voc side."""
    return v_mp + (v_oc - v_mp) * math.sqrt(max(0.0, 1.0 - p_dc / max(p_dc_mpp, 1.0)))


def _solar_events(rng: random.Random, t0: float, duration: float, grid: _Grid) -> List[Event]:
    """Four SunSpec model 103 inverters; INV03 trips on a ground fault (see the module doc)."""
    n = max(0, int(math.ceil(duration)))
    cloud = _OU(random.Random(rng.getrandbits(64)), 240.0, 0.035)
    inverter_rngs = [random.Random(rng.getrandbits(64)) for _ in range(4)]
    sky = [0.80 + cloud.step() for _ in range(n + 2)]  # thin high cloud, shared by the park
    out = []
    peak_kwp = 2750.0
    for idx, offset, rng in zip(range(1, 5), (4.0, 11.5, 19.0, 26.5), inverter_rngs):
        inv = f"inv{idx:02d}"
        mismatch = rng.uniform(0.985, 1.01)
        energy_kwh = rng.uniform(17.55e6, 18.25e6)  # lifetime, about 6.5 years in service
        cab_off, v_ac_off = rng.uniform(-1.0, 1.0), rng.uniform(-0.003, 0.003)
        tmp_cab: Optional[float] = None  # lagging thermal state, degC
        tmp_snk = 0.0
        for at in _at_list(offset, 30.0, duration):
            unix = t0 + at
            i = min(len(sky) - 1, int(at))
            poa = _poa(unix) * sky[i] * mismatch
            t_amb = _ambient(unix)
            t_mod = t_amb + 0.018 * poa
            p_dc_mpp = peak_kwp * poa / 1000.0 * (1 - 0.0035 * (t_mod - 25.0)) * 0.925
            g = max(poa, 1.0) / 1000.0
            v_mp = 1150.0 * (1 - 0.0029 * (t_mod - 25.0)) * (1 + 0.04 * math.log(g)) if poa > 1 else 0.0
            v_oc = v_mp * 1.21 if poa > 1 else 35.0

            st, st_vnd, evt1, evt_vnd = 4, 4001, 0, 0
            p_dc, v_dc = p_dc_mpp, v_mp
            avail_ac = _inverter_ac_kw(p_dc_mpp)
            if avail_ac > INV_RATED_KVA:  # clipping at the nameplate
                st, st_vnd = 5, 5020
                p_dc = _inverter_dc_kw(INV_RATED_KVA)
                v_dc = _off_mpp_v(v_mp, v_oc, p_dc, p_dc_mpp)
            if p_dc_mpp < 5.0:
                st, st_vnd, p_dc, v_dc = 2, 2000, 0.0, v_oc if poa > 1 else 35.0
            if inv == "inv03" and at >= INV_FAULT_AT and p_dc_mpp >= 5.0:
                if at < INV_RETEST_AT:
                    st, st_vnd, evt1, evt_vnd, p_dc, v_dc = 7, 7104, 1, 4096, 0.0, v_oc
                elif at < INV_RESTART_AT:
                    st, st_vnd, p_dc, v_dc = 8, 8060, 0.0, v_oc
                elif at < INV_RAMP_AT:
                    st, st_vnd, p_dc, v_dc = 3, 3000, 0.0, v_oc
                else:
                    cap = INV_RAMP_KW_PER_S * (at - INV_RAMP_AT)
                    if cap < min(avail_ac, INV_RATED_KVA):
                        st, st_vnd = 5, 5010
                        # the inverter backs off its MPP towards open circuit
                        p_dc = cap * 1.02
                        v_dc = _off_mpp_v(v_mp, v_oc, p_dc, p_dc_mpp)
            v_dc += rng.gauss(0.0, 1.5) if p_dc > 0 else rng.gauss(0.0, 0.5)
            p_ac = min(INV_RATED_KVA, _inverter_ac_kw(p_dc) * (1 + rng.gauss(0.0, 0.0015))) if p_dc > 0 else 0.0
            q_kvar = rng.gauss(0.0, 1.5) if p_ac > 0 else 0.0
            s_kva = math.sqrt(p_ac * p_ac + q_kvar * q_kvar)
            v_ll = 645.0 * (1 + v_ac_off) * grid.u(at) / 51.35
            hz = grid.f(at) + rng.gauss(0.0, 0.002)
            energy_kwh += p_ac * 30.0 / 3600.0
            i_ac = s_kva * 1000.0 / (math.sqrt(3) * v_ll) if s_kva > 0 else 0.0
            i_dc = p_dc * 1000.0 / v_dc if p_dc > 0 else 0.0
            # The heat sink and the cabinet follow the losses (about proportional to the
            # output) with a lag; the station is heated to about 20 degC in cold weather.
            cab_target = max(19.5, t_amb + 5.0) + cab_off + 0.004 * p_ac
            snk_target = max(22.0, t_amb + 7.5) + 0.012 * p_ac
            if tmp_cab is None:
                tmp_cab, tmp_snk = cab_target, snk_target
            else:
                tmp_cab += (cab_target - tmp_cab) * (1.0 - math.exp(-30.0 / INV_TAU_CAB_S))
                tmp_snk += (snk_target - tmp_snk) * (1.0 - math.exp(-30.0 / INV_TAU_SINK_S))
            payload = {
                "ts": ts_ms(t0, at),
                "model": 103,
                "A": int(round(i_ac * 10)), "A_SF": -1,
                "PPVphAB": int(round(v_ll * 10)),
                "PPVphBC": int(round(v_ll * 10 * (1 + rng.gauss(0.0, 0.0008)))),
                "PPVphCA": int(round(v_ll * 10 * (1 + rng.gauss(0.0, 0.0008)))),
                "V_SF": -1,
                "W": int(round(p_ac * 10)), "W_SF": 2,
                "Hz": int(round(hz * 100)), "Hz_SF": -2,
                "VA": int(round(s_kva * 10)), "VA_SF": 2,
                "VAr": int(round(q_kvar * 10)), "VAr_SF": 2,
                "PF": int(round(p_ac / s_kva * 10000)) if s_kva > 0 else -32768, "PF_SF": -2,
                "WH": int(energy_kwh), "WH_SF": 3,
                "DCA": int(round(i_dc * 10)), "DCA_SF": -1,
                "DCV": int(round(max(0.0, v_dc) * 10)), "DCV_SF": -1,
                "DCW": int(round(p_dc * 10)), "DCW_SF": 2,
                "TmpCab": int(round((tmp_cab + rng.gauss(0.0, 0.05)) * 10)),
                "TmpSnk": int(round((tmp_snk + rng.gauss(0.0, 0.1)) * 10)),
                "TmpTrns": -32768,
                "TmpOt": -32768,
                "Tmp_SF": -1,
                "St": st, "StVnd": st_vnd,
                "Evt1": evt1, "Evt2": 0, "EvtVnd1": evt_vnd,
            }
            out.append(Event(at=at, client=f"{PV}-gw", topic=f"plant/{PV}/{inv}/sunspec", payload=jbytes(payload)))
    return out


# --------------------------------------------------------------------------- peaker


def _peaker_events(rng: random.Random, t0: float, duration: float, grid: _Grid) -> List[Event]:
    """The gas-turbine peaker's RTU: one CSV line every 15 s (format in the module doc)."""
    out = []
    for at in _at_list(2.0, 15.0, duration):
        ts = int(math.floor(t0 + at))
        mw: Optional[float] = 0.0
        mvar: Optional[float] = None
        kv, hz, breaker = 0.0, None, 1
        if at >= PEAKER_EXCITED:
            kv = max(0.0, 10.50 * min(1.0, (at - PEAKER_EXCITED) / 10.0) + rng.gauss(0.0, 0.004))
            hz = min(grid.f(at) + 0.03, 49.5 + 0.25 * (at - PEAKER_EXCITED)) + rng.gauss(0.0, 0.002)
            mvar = 0.0
        if at >= PEAKER_SYNC:
            breaker = 2
            mw = min(PEAKER_TARGET_MW, PEAKER_RAMP_MW_PER_S * (at - PEAKER_SYNC)) + rng.gauss(0.0, 0.08)
            mvar = 1.5 + 0.12 * mw + rng.gauss(0.0, 0.15)
            kv = 10.52 + 0.002 * mw + rng.gauss(0.0, 0.004)
            hz = grid.f(at) + rng.gauss(0.0, 0.001)

        def num(x: Optional[float], nd: int) -> str:
            return "" if x is None else f"{_r(x, nd):.{nd}f}"

        line = f"{ts},G1,{num(mw, 1)},{num(mvar, 1)},{num(kv, 2)},{num(hz, 3)},{breaker}"
        if abs(at - RTU_GLITCH_AT) < 1e-6:  # the serial line drops the rest of this poll
            f = line.split(",")
            line = ",".join(f[:3]) + "," + f[3][:2]
        out.append(Event(at=at, client=f"{PK}-rtu1", topic=f"plant/{PK}/g1/rtu", payload=line.encode()))
    return out
