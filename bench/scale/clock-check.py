#!/usr/bin/env python3
"""Validate retained chrony tracking reports; never infer sync from installation."""

import argparse
import datetime
import json
import math
from pathlib import Path


MAX_REFERENCE_ABSOLUTE_MS = 50.0
# Liveness bounds, not accuracy gates — accuracy is `max_error_ms`, which already
# grows with staleness (see `tracking`). A fleet host polls the reference every
# <=16 s (clock-sync.sh pins `maxpoll 4`) but its polls are DELAYED under load;
# the reference polls the internet on chrony's own schedule, up to 1024 s.
FLEET_MAX_AGE_S = 900
REFERENCE_MAX_AGE_S = 2 * 1024 + 60


def tracking(text, max_error_ms=5.0, max_age_s=FLEET_MAX_AGE_S, reference=None):
    """One host's chrony tracking report -> its clock error bounds.

    `reference` is the parsed report of the fleet's NTP reference host, or None
    when this host IS the reference. Two bounds come back:

    absolute_error_ms = |system offset| + root delay / 2 + root dispersion
        chrony's documented bound against TRUE time. Root delay is the round trip
        all the way to the external source.

    error_ms (the GATED one) — the bound against the FLEET's common timescale.
        Every host disciplines to one reference over the private network, so the
        reference's own path to the external source (its root delay, ~7 ms on a
        Hetzner fleet) is inherited identically by every host: it moves all the
        clocks together and cancels in any difference between two of them, which
        is the only thing a cross-host latency is. What does not cancel is the hop
        from this host to the reference:
            |system offset| + (root delay - reference root delay) / 2 + root dispersion
        Root dispersion is kept WHOLE rather than differenced — the two reports
        are captured at slightly different moments, and the conservative side of
        that is to charge the host all of it. The reference itself is off the
        fleet timescale only by its own outstanding slew, |system offset|.

    The 2026-09-19 fleet shows why the absolute bound cannot gate: driver6 read
    0.013 + 7.881/2 + 1.102 = 5.056 ms against a 5 ms budget and the run died
    before offering load, while its error against broker0 was
    0.013 + (7.881 - 7.309)/2 + 1.102 = 1.401 ms.
    """
    fields = dict(line.split(":", 1) for line in text.splitlines() if ":" in line)
    fields = {k.strip(): v.strip() for k, v in fields.items()}
    if fields.get("Leap status") != "Normal":
        raise ValueError("clock is not synchronized (normal leap status required)")
    stratum = int(fields["Stratum"])
    if not 1 <= stratum <= 15:
        raise ValueError("invalid NTP stratum")
    if fields["Reference ID"].split()[0] in ("00000000", "7F7F0101"):
        raise ValueError("clock has no external reference")
    offset, root_delay, root_dispersion = (float(fields[k].split()[0]) for k in
                                           ("System time", "Root delay", "Root dispersion"))
    if not all(math.isfinite(v) for v in (offset, root_delay, root_dispersion)) or min(root_delay, root_dispersion) < 0:
        raise ValueError("invalid clock uncertainty")
    absolute_ms = 1000 * (abs(offset) + root_delay / 2 + root_dispersion)
    if reference is None:
        # The reference's absolute error cancels between hosts, but only while
        # the reference is itself a steady clock: one with a wild upstream is
        # being stepped and slewed, and the fleet chases it. A sanity ceiling,
        # deliberately looser than the latency budget and far from unlimited.
        if absolute_ms > MAX_REFERENCE_ABSOLUTE_MS:
            raise ValueError(f"fleet reference is {absolute_ms:.3f}ms from true time (limit {MAX_REFERENCE_ABSOLUTE_MS:g}ms)")
        error_ms = 1000 * abs(offset)
    else:
        # The cancellation is only valid for a host that really is one hop below
        # the reference. Anything else has an unknown path and gets no discount.
        if stratum != reference["stratum"] + 1:
            raise ValueError("clock is not disciplined to the fleet reference")
        # The difference estimates this host's own hop, and it is CLAMPED at zero
        # rather than trusted to be positive. The two reports are snapshots taken
        # moments apart and the reference's internet path varies, so a host that
        # synced before the reference's delay grew records a smaller total than
        # the reference now shows. Measured 2026-09-19 at 120,000 msg/s: seven of
        # ten fleet hosts were below the reference's 8.344 ms, by up to 0.46 ms,
        # and treating that as "not disciplined" killed the run. The whole
        # dispersion below is the conservative term that covers this.
        hop = max(0.0, root_delay - reference["root_delay_s"])
        error_ms = 1000 * (abs(offset) + hop / 2 + root_dispersion)
    at = float(fields["Captured epoch"])
    ref = datetime.datetime.strptime(fields["Ref time (UTC)"], "%a %b %d %H:%M:%S %Y").replace(tzinfo=datetime.timezone.utc).timestamp()
    age = at - ref
    # Staleness is ALREADY PRICED IN above: chrony grows root dispersion with the
    # time since its last measurement, so `error_ms` rises on its own as a sample
    # ages, and the 5 ms budget fails a genuinely stale clock without help. This
    # check is therefore a LIVENESS bound — has chrony stopped entirely — and not
    # a second, cruder version of the same gate.
    #
    # Measured on the 2026-09-19 fleet, which a 120 s limit killed twice: under
    # load the fleet's NTP polls are delayed, so the last sample aged 31 -> 132 s
    # across one rung while the worst fleet error only moved 0.99 -> 1.66 ms —
    # comfortably inside budget. Staleness costs ~12 us/s here, so a clock that is
    # stale ENOUGH to matter breaches the error budget after ~5 minutes anyway.
    if reference is None:
        max_age_s = max(max_age_s, REFERENCE_MAX_AGE_S)
    if not math.isfinite(at) or not 0 <= age <= max_age_s:
        raise ValueError("stale/future NTP reference")
    if not math.isfinite(max_error_ms) or max_error_ms <= 0 or error_ms > max_error_ms:
        raise ValueError(f"clock error bound {error_ms:.3f}ms exceeds {max_error_ms:g}ms")
    return {"error_ms": error_ms, "absolute_error_ms": absolute_ms, "reference_age_s": age,
            "reference": fields["Reference ID"], "captured_epoch": at,
            "stratum": stratum, "root_delay_s": root_delay}


def validate_phase(directory, hosts, max_error_ms=5.0):
    """`hosts[0]` is the fleet's NTP reference (clock-sync.sh points every other
    host at the first broker); the rest are gated on their error against it."""
    reference = tracking((directory / f"{hosts[0]}.txt").read_text(), max_error_ms)
    report = {hosts[0]: reference}
    for host in hosts[1:]:
        report[host] = tracking((directory / f"{host}.txt").read_text(), max_error_ms, reference=reference)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("max_error_ms", type=float)
    parser.add_argument("hosts", nargs="+")
    args = parser.parse_args()
    try:
        print(json.dumps(validate_phase(args.directory, args.hosts, args.max_error_ms), indent=2))
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(1, f"INVALID CLOCK: {exc}\n")
