#!/usr/bin/env python3
"""Adverse evidence tests: each corruption independently invalidates a good run."""

import base64
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


def module(name, file):
    s = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


E = module("e", "lane-e-evidence.py")
S = module("s", "summarize-curve.py")


class Evidence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.p = Path(self.tmp.name) / "nodes=1/laneE/sites-1"
        self.p.mkdir(parents=True)
        p = self.p
        self.pub = "pub-s0-0"
        self.subs = ["sub-s0-0", "sub-s0-1"]
        eps = [
            {"name": self.pub, "driver": 0, "site": "0", "role": "pub", "clients": 1}
        ] + [
            {"name": n, "driver": 0, "site": "0", "role": "sub", "clients": 1}
            for n in self.subs
        ]
        (p / "manifest.json").write_text(
            json.dumps({"nodes": 1, "drivers": 1, "window_secs": 60, "endpoints": eps})
        )
        (p / "rung.txt").write_text(
            "sites=1 offered=10 publishers=1 consumers=2 per_consumer=5 qos=1 sub_qos=1 window_secs=60 window=aligned cpu_window=missing settled=yes reset=yes steady=yes drained=yes p99_budget_ms=1000"
        )
        (p / "window.tsv").write_text(
            "host\tphase\tstart_ms\tend_ms\tmain_pid\nbroker0\topen\t100000\t100010\t42\nbroker0\tclose\t160000\t160010\t42\ndriver0\topen\t100000\t100010\t\ndriver0\tclose\t160000\t160010\t\n"
        )
        for phase, n in [
            ("before", 0),
            ("window-open", 0),
            ("window-close", 600),
            ("drain", 600),
            ("after", 600),
        ]:
            (p / f"metrics-{phase}-broker0.prom").write_text(
                f'mqttd_publish_received_total{{qos="1"}} {n}\nmqttd_publish_delivered_total{{qos="1"}} {n}\nmqttd_peer_links 0\n# EOF\n'
            )
        for name in [self.pub] + self.subs:
            for suffix, final in [("-base", False), ("", True), ("-terminal", True)]:
                n = (600 if name == self.pub else 300) if final else 0
                family = "puback_latency" if name == self.pub else "e2e_latency"
                txt = (
                    f"audit_sent {n}\naudit_acked {n}\naudit_paused_workers {1 if final else 0}\npub_overrun 0\n"
                    if name == self.pub
                    else f"recv {n}\n"
                )
                txt += f'{family}_bucket{{le="1"}} {n}\n{family}_bucket{{le="+Inf"}} {n}\n{family}_count {n}\n'
                (p / f"{name}{suffix}.prom").write_text(txt + "# EOF\n")
            kinds = ["sent", "acked"] if name == self.pub else ["received"]
            bits = (
                (1 << 600) - 1
                if name == self.pub
                else sum(1 << i for i in range(self.subs.index(name), 600, 2))
            )
            rows = [
                f"{k}\t{base64.b64encode(b'site/0/1').decode()}\t<0.1.0>\t{bits.bit_count()}\t{bits:X}\n"
                for k in kinds
            ]
            (p / f"{name}.ledger").write_text("".join(rows) + "# EOF\n")

    def test_valid(self):
        r = E.validate(self.p)
        self.assertEqual(r["counts"]["unique_delivered"], 600)
        self.assertEqual(r["sent_rate"], 10)
        self.assertTrue(S.lane_e_rung(self.p)["pass"])

    def test_missing_each_artifact(self):
        for f in list(self.p.iterdir()):
            if f.name == "rung.txt":
                continue
            with self.subTest(file=f.name):
                data = f.read_bytes()
                f.unlink()
                with self.assertRaises((ValueError, OSError, KeyError)):
                    E.validate(self.p)
                f.write_bytes(data)

    def test_histogram_corruption(self):
        f = self.p / (self.subs[0] + ".prom")
        original = f.read_text()
        for txt in [
            original.replace("count 300", "count 301"),
            original.replace('le="1"} 300', 'le="1"} 301'),
            original.replace('le="1"} 300', 'le="1"} NaN'),
        ]:
            f.write_text(txt)
            with self.assertRaises(ValueError):
                E.validate(self.p)
        f.write_text(original)

    def test_counter_reset(self):
        f = self.p / (self.pub + "-base.prom")
        f.write_text(f.read_text().replace("audit_sent 0", "audit_sent 601"))
        with self.assertRaises(ValueError):
            E.validate(self.p)

    def test_ledger_loss_balanced_by_duplicate(self):
        f = self.p / (self.subs[0] + ".ledger")
        txt = f.read_text()
        row = txt.splitlines()[0].split("\t")
        row[4] = f"{int(row[4], 16) & ~1:X}"
        f.write_text("\t".join(row) + "\n# EOF\n")
        with self.assertRaisesRegex(ValueError, "ledger does not close"):
            E.validate(self.p)

    def test_qos_downgrade_and_drop_gate(self):
        f = self.p / "metrics-after-broker0.prom"
        original = f.read_text()
        for txt in [
            original.replace('delivered_total{qos="1"}', 'delivered_total{qos="0"}'),
            original.replace(
                "# EOF", 'mqttd_publish_dropped_total{reason="pending-cap"} 1\n# EOF'
            ),
        ]:
            f.write_text(txt)
            self.assertFalse(S.lane_e_rung(self.p)["pass"])
        f.write_text(original)

    def test_drain_and_steady_gate(self):
        f = self.p / "rung.txt"
        original = f.read_text()
        for token in ["drained", "steady", "settled", "reset"]:
            f.write_text(original.replace(token + "=yes", token + "=no"))
            self.assertFalse(S.lane_e_rung(self.p)["pass"])
        f.write_text(original)

    def telemetry_fixture(self):
        p = self.p
        manifest = json.loads((p / "manifest.json").read_text())
        manifest["telemetry_required"] = True
        (p / "manifest.json").write_text(json.dumps(manifest))
        (p / ".batch").mkdir()
        for phase, t, n in [
            ("open", 100000, 0),
            ("sample-1", 110000, 100),
            ("sample-2", 130000, 300),
            ("sample-3", 150000, 500),
            ("close", 160000, 600),
        ]:
            (p / ".batch" / f"{phase}-broker0").write_text(
                "mqttd_backlog_bytes 0\nmqttd_inflight_messages 0\n# EOF\n"
            )
            parts = [f"WINDOW_STAMP_MS {t}\n"]
            for ep in manifest["endpoints"]:
                text = (
                    (p / (ep["name"] + ".prom"))
                    .read_text()
                    .replace("600", str(n))
                    .replace("300", str(n // 2))
                )
                # Only counters consumed by telemetry are needed here.
                text = (
                    f"audit_sent {n}\naudit_acked {n}\n"
                    if ep["role"] == "pub"
                    else f"recv {n // 2}\n"
                )
                parts.append("@@@ " + ep["name"] + "\n" + text)
            parts.append(f"WINDOW_STAMP_MS {t + 10}\n")
            (p / ".batch" / f"{phase}-driver0").write_text("".join(parts))

    def test_periodic_sample_is_mandatory(self):
        self.telemetry_fixture()
        self.assertTrue(E.validate(self.p)["telemetry"]["queues_bounded"])
        (self.p / ".batch/sample-2-driver0").unlink()
        with self.assertRaises(OSError):
            E.validate(self.p)

    def test_queue_growth_fails(self):
        self.telemetry_fixture()
        f = self.p / ".batch/close-broker0"
        f.write_text(f.read_text().replace("bytes 0", "bytes 100000"))
        self.assertFalse(S.lane_e_rung(self.p)["pass"])

    def test_shared_reconcile(self):
        self.assertEqual(E.reconcile({"t": 7}, {"t": 7}, {"t": 7})["missing"], 0)
        self.assertEqual(E.reconcile({"t": 7}, {"t": 7}, {"t": 3})["missing"], 1)

    def test_sparse_log_counter(self):
        f = self.p / "pub.log"
        f.write_text(
            "0s pub total=0 rate=0/sec\n10s pub_overrun total=100 rate=10/sec\n60s pub total=600 rate=10/sec\n2m0s pub total=1200 rate=10/sec\n"
        )
        self.assertEqual(S.driver_rate(f, "pub_overrun"), (100, 0))
        self.assertEqual(S.driver_rate(f, "pub", 120), (1200, 10))


if __name__ == "__main__":
    unittest.main()
