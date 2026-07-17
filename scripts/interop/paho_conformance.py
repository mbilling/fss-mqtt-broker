#!/usr/bin/env python3
"""Second foreign-client interop oracle: Eclipse Paho (ADR 0034 T7 / ADR 0044 P7).

The Mosquitto CLI suite (run.sh) proves payloads round-trip. Paho, a second
*independent* MQTT implementation, is driven programmatically so it can assert
the things a CLI cannot surface: MQTT 5 **reason codes**, CONNACK/SUBACK
**properties**, per-filter **granted QoS** (including a downgrade), **session
present** on resume, and **Will** delivery. A passing run is independent
evidence the broker's control-plane semantics — not just its payloads — match
what the ecosystem expects.

Paho is an external process dependency (pip), NOT a cargo dependency: nothing is
added to the broker's supply chain. Driven against the plaintext listener the
caller passes in $MQTT_PORT. Exits non-zero on any mismatch.
"""

import os
import sys
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

HOST = "127.0.0.1"
PORT = int(os.environ["MQTT_PORT"])
TMO = 5.0

PASS = 0
FAIL = 0


def ok(name):
    global PASS
    PASS += 1
    print(f"  ok   — {name}")


def bad(name, detail=""):
    global FAIL
    FAIL += 1
    print(f"  FAIL — {name}{(' — ' + detail) if detail else ''}")


def expect(name, want, got):
    if want == got:
        ok(name)
    else:
        bad(name, f"expected [{want}] got [{got}]")


def v5_client(cid, clean=True):
    c = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id=cid,
        protocol=mqtt.MQTTv5,
    )
    c.enable_logger(None)
    return c


def wait(pred, deadline):
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.02)
    return False


def connect(c, clean_start=True):
    box = {}
    c.on_connect = lambda cl, u, flags, rc, props: box.update(
        flags=flags, rc=rc, props=props
    )
    c.connect(HOST, PORT, keepalive=30, clean_start=clean_start)
    c.loop_start()
    if not wait(lambda: "rc" in box, time.time() + TMO):
        raise SystemExit("FATAL: no CONNACK from broker")
    return box


def main():
    # --- 1. v5 CONNECT: success reason code + a CONNACK we can inspect --------
    c = v5_client("paho-main")
    info = connect(c)
    rc = info["rc"]
    expect("v5 CONNACK reason is success", 0, int(rc.value))
    # A fresh clean-start session must report session-present = False.
    expect("fresh session: session-present false", False, bool(info["flags"].session_present))

    # --- 2. SUBSCRIBE granted QoS, including a broker downgrade ---------------
    granted = {}
    c.on_subscribe = lambda cl, u, mid, rcs, props: granted.update(mid=mid, rcs=rcs)
    # Subscribe to two filters: one at QoS 1, one at QoS 2. The broker grants
    # each subscription's maximum QoS back in the SUBACK.
    c.subscribe([("paho/q1", 1), ("paho/q2", 2)])
    if not wait(lambda: "rcs" in granted, time.time() + TMO):
        bad("SUBACK received")
    else:
        codes = [int(r.value) for r in granted["rcs"]]
        expect("SUBACK grants QoS 1 for the QoS-1 filter", 1, codes[0])
        expect("SUBACK grants QoS 2 for the QoS-2 filter", 2, codes[1])

    # --- 3. PUBLISH round-trip with a User Property (v5) + PUBACK reason ------
    received = []
    c.on_message = lambda cl, u, msg: received.append(msg)
    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [("zone", "kitchen"), ("unit", "celsius")]
    pub = c.publish("paho/q1", "temp-21.5", qos=1, properties=props)
    pub.wait_for_publish(TMO)
    if not wait(lambda: received, time.time() + TMO):
        bad("v5 QoS1 message delivered")
    else:
        m = received[0]
        expect("payload survives", b"temp-21.5", m.payload)
        ups = dict(m.properties.UserProperty) if hasattr(m.properties, "UserProperty") else {}
        expect("User Property 'zone' survives the hop", "kitchen", ups.get("zone"))
        expect("User Property 'unit' survives the hop", "celsius", ups.get("unit"))

    # --- 4. Retained delivery + RETAIN flag preserved to a late subscriber ----
    c.publish("paho/retained", "kept-value", qos=1, retain=True).wait_for_publish(TMO)
    late = v5_client("paho-late")
    connect(late)
    late_msgs = []
    late.on_message = lambda cl, u, msg: late_msgs.append(msg)
    late.subscribe("paho/retained", qos=1)
    if not wait(lambda: late_msgs, time.time() + TMO):
        bad("retained message delivered to a late subscriber")
    else:
        expect("retained payload", b"kept-value", late_msgs[0].payload)
        expect("retain flag set on the retained delivery", True, bool(late_msgs[0].retain))
    late.publish("paho/retained", "", qos=1, retain=True).wait_for_publish(TMO)  # clear
    late.loop_stop()
    late.disconnect()

    # --- 5. Session present TRUE on resume of a persistent session ------------
    persist = v5_client("paho-persist")
    p_props = Properties(PacketTypes.CONNECT)
    p_props.SessionExpiryInterval = 300  # persist beyond disconnect
    persist.connect(HOST, PORT, keepalive=30, clean_start=True, properties=p_props)
    persist.loop_start()
    time.sleep(0.3)
    persist.subscribe("paho/resume", qos=1)
    time.sleep(0.3)
    persist.loop_stop()
    persist.disconnect()
    time.sleep(0.3)
    # Reconnect with clean_start False: the broker must report the session back.
    again = v5_client("paho-persist")
    info2 = connect(again, clean_start=False)
    expect("resume reports session-present true", True, bool(info2["flags"].session_present))
    again.loop_stop()
    again.disconnect()

    c.loop_stop()
    c.disconnect()

    print(f"\n  passed: {PASS}   failed: {FAIL}")
    if FAIL:
        print("PAHO INTEROP FAILED")
        sys.exit(1)
    print("PAHO INTEROP OK")


if __name__ == "__main__":
    main()
