"""Play simulated power-plant, home and car telemetry into mqttd, and watch what rules derive.

    python3 simulate.py                              # all domains, live, 10x real time
    python3 simulate.py --domains cars --speed 1     # one domain at real time
    python3 simulate.py --dry-run --start 2026-01-15T07:30:00Z > fixture.jsonl
    python3 simulate.py --replay fixture.jsonl --speed 0

Live mode connects one MQTT client per simulated device to --host/--port and, unless
--no-watch, subscribes to the topics the demo's rules publish to and prints each derived
message as it arrives. The data is seeded: the same --seed, --start and --duration give
the same messages, byte for byte. See README.md for the devices, the rules and why.
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sim import cars, homes, power  # noqa: E402
from sim.core import Event, Player, from_json_line, merge, to_json_line  # noqa: E402
from sim.mqtt import Client, MqttError  # noqa: E402

DOMAINS = {"power": power, "homes": homes, "cars": cars}
# Where the demo's rules publish (README.md, "Where the results go").
DERIVED = ["alerts/#", "kpi/#", "normalized/#", "analytics/#", "state/#", "events/#"]


def parse_start(text: str) -> float:
    if text == "now":
        return float(int(time.time()))
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def generate(domains: list[str], seed: int, t0: float, duration: float) -> list[Event]:
    streams = []
    for name in domains:
        rng = random.Random(f"{seed}:{name}")
        streams.append(DOMAINS[name].events(rng, t0, duration))
    return merge(streams)


def show(prefix: str, topic: str, payload: bytes, width: int = 160) -> str:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        text = "0x" + payload.hex()
    line = f"{prefix} {topic}  {text}"
    return line if len(line) <= width else line[: width - 1] + "…"


def watch(host: str, port: int, filters: list[str], lock: threading.Lock, stop: threading.Event,
          counts: dict[str, int]):
    c = Client(host, port, f"demo-watch-{int(time.time())}")
    c.connect()
    c.subscribe([(f, 1) for f in filters])
    ready.set()
    while not stop.is_set():
        try:
            m = c.poll(0.2)
        except MqttError as e:
            with lock:
                print(f"watcher stopped: {e}", file=sys.stderr)
            return
        if m:
            root = m.topic.split("/", 1)[0]
            counts[root] = counts.get(root, 0) + 1
            with lock:
                print(show("  ⇒", m.topic, m.payload), flush=True)
    c.disconnect()


ready = threading.Event()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--domains", default="power,homes,cars", help="comma-separated: power,homes,cars")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--duration", type=float, default=600, help="simulated seconds (default 600)")
    ap.add_argument("--start", default="now", help="simulated start, ISO 8601 UTC, or 'now'")
    ap.add_argument("--speed", type=float, default=10, help="times real time; 0 = as fast as possible")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--dry-run", action="store_true", help="print the events as JSON lines; no broker")
    ap.add_argument("--replay", metavar="FILE", help="play a file written by --dry-run")
    ap.add_argument("--no-watch", action="store_true", help="do not print what the rules derive")
    ap.add_argument("--quiet", action="store_true", help="do not print each device message sent")
    args = ap.parse_args()

    if args.replay:
        with open(args.replay, encoding="utf-8") as f:
            events = [from_json_line(line) for line in f if line.strip()]
    else:
        names = [d.strip() for d in args.domains.split(",") if d.strip()]
        unknown = [d for d in names if d not in DOMAINS]
        if unknown:
            ap.error(f"unknown domain(s): {', '.join(unknown)}")
        events = generate(names, args.seed, parse_start(args.start), args.duration)

    if args.dry_run:
        for ev in events:
            print(to_json_line(ev))
        return 0

    lock = threading.Lock()
    stop = threading.Event()
    counts: dict[str, int] = {}
    watcher = None
    if not args.no_watch:
        watcher = threading.Thread(
            target=watch, args=(args.host, args.port, DERIVED, lock, stop, counts), daemon=True
        )
        watcher.start()
        if not ready.wait(10):
            print("could not subscribe to the derived topics", file=sys.stderr)
            return 1

    sent = {"n": 0}

    def on_send(ev: Event):
        if ev.kind == "publish":
            sent["n"] += 1
            if not args.quiet:
                with lock:
                    print(show(f"{ev.client:>14} →", ev.topic, ev.payload), flush=True)

    player = Player(args.host, args.port, args.speed, on_send)
    try:
        player.play(events)
    except (MqttError, OSError) as e:
        print(f"publishing failed: {e}", file=sys.stderr)
        return 1
    finally:
        player.close()
    if watcher:
        time.sleep(1.0)  # let the last derived messages arrive
        stop.set()
        watcher.join(5)
        derived = sum(counts.values())
        by_root = ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
        print(f"\n{sent['n']} device messages in, {derived} derived out ({by_root})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
