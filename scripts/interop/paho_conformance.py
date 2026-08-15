#!/usr/bin/env python3
"""Second foreign-client interop oracle: Eclipse Paho (ADR 0034 T7 / ADR 0044 P7).

The Mosquitto CLI suite (run.sh) proves payloads round-trip. Paho, a second
*independent* MQTT implementation, is driven programmatically so it can assert
the things a CLI cannot surface: MQTT 5 **reason codes**, CONNACK/SUBACK
**properties**, per-filter **granted QoS** (including a downgrade), **session
present** on resume, **Will** delivery — including the MQTT 5 **Will Delay
Interval** (0x18) and its cancellation by a resume, issue #299 — and the broker's
**capability advertisement** (Subscription Identifiers Available = 0) together with
the DISCONNECT 0xA1 refusal that must accompany it. A passing run is independent
evidence the broker's control-plane semantics — not just its payloads — match
what the ecosystem expects.

The Will Delay case earns its ~9 s of lane time: the property round-tripped in the
broker's own codec with NO reader anywhere outside it, and every test this project
wrote itself stayed green. It took the Eclipse `paho.mqtt.testing` suite to notice.

Paho is an external process dependency (pip), NOT a cargo dependency: nothing is
added to the broker's supply chain. Driven against the plaintext listener the
caller passes in $MQTT_PORT. Exits non-zero on any mismatch.
"""

import os
import socket
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


def _varint(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        out.append(b | 0x80 if n else b)
        if not n:
            return bytes(out)


def _str(b):
    return len(b).to_bytes(2, "big") + b


def raw_subscribe_with_identifier():
    """Hand-build a v5 CONNECT + SUBSCRIBE carrying Subscription Identifier 7 on a bare
    socket, and return the broker's reply frame as hex.

    Needed only because paho 2.1.0 cannot surface a 2-byte DISCONNECT's reason code
    (see the note at the call site). Expected reply: `e0 02 a1 00` — DISCONNECT,
    Remaining Length 2, reason 0xA1, zero-length property block.
    """
    body = (
        b"\x00\x04MQTT"  # protocol name
        b"\x05"  # protocol level 5
        b"\x02"  # connect flags: clean start
        + (30).to_bytes(2, "big")  # keep alive
        + b"\x00"  # zero-length CONNECT properties
        + _str(b"raw-subid")  # client id
    )
    connect_pkt = b"\x10" + _varint(len(body)) + body

    props = b"\x0b\x07"  # 0x0B Subscription Identifier = 7
    body = (
        (1).to_bytes(2, "big")  # packet id
        + _varint(len(props))
        + props
        + _str(b"raw/subid")  # topic filter
        + b"\x01"  # options: QoS 1
    )
    subscribe_pkt = b"\x82" + _varint(len(body)) + body

    sock = socket.create_connection((HOST, PORT), TMO)
    try:
        sock.settimeout(TMO)
        sock.sendall(connect_pkt)
        if not sock.recv(256):
            return "no CONNACK"
        sock.sendall(subscribe_pkt)
        return sock.recv(256).hex()
    finally:
        sock.close()


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

    # --- 6. Subscription Identifiers: advertised unavailable, and refused -----
    #
    # MQTT 5.0 §3.2.2.3.12, verbatim: "If not present, then Subscription Identifiers
    # are supported." This broker does not deliver them, so it must say 0 — and must
    # then refuse any SUBSCRIBE that uses one (issue #245). Paho does no client-side
    # validation of property 41 (there is no `SubscriptionIdentifier` reference
    # anywhere in client.py), so it genuinely puts the property on the wire — which is
    # what makes this drivable by a real foreign client and not just our own codec.
    expect(
        "CONNACK advertises Subscription Identifiers unavailable",
        0,
        getattr(info["props"], "SubscriptionIdentifierAvailable", None),
    )

    # A DEDICATED client: this case ends in a server-initiated disconnect, which would
    # poison `paho-main` for the sections above. `reconnect_on_failure=False` stops the
    # loop thread from re-dialling after the broker closes us.
    sub = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id="paho-subid",
        protocol=mqtt.MQTTv5,
        reconnect_on_failure=False,
    )
    sub.enable_logger(None)
    seen = {"subacks": 0}
    sub.on_subscribe = lambda cl, u, mid, rcs, props: seen.update(
        subacks=seen["subacks"] + 1
    )
    # VERSION2 signature (paho 2.x `_do_on_disconnect`):
    #   on_disconnect(client, userdata, disconnect_flags, reason_code, properties)
    sub.on_disconnect = lambda cl, u, flags, rc, props: seen.update(
        dc_reason=int(rc.value), from_server=bool(flags.is_disconnect_packet_from_server)
    )
    connect(sub)
    sub_props = Properties(PacketTypes.SUBSCRIBE)
    sub_props.SubscriptionIdentifier = 7
    sub.subscribe("paho/subid", qos=1, properties=sub_props)
    if not wait(lambda: "dc_reason" in seen, time.time() + TMO):
        bad(
            "SUBSCRIBE with a Subscription Identifier is refused",
            "no DISCONNECT arrived",
        )
    else:
        expect(
            "a real client's identifier-bearing SUBSCRIBE is disconnected by the server",
            True,
            seen.get("from_server"),
        )
        expect("no SUBACK preceded the refusal", 0, seen["subacks"])
    sub.loop_stop()

    # The reason BYTE, read off the wire directly, because Paho cannot report it:
    # `Client._handle_disconnect` (paho 2.1.0) only decodes the reason code when the
    # DISCONNECT's Remaining Length is > 2, and a reason-plus-empty-properties
    # DISCONNECT is exactly 2 (`e0 02 a1 00`). Its own inline comment says "if reason
    # is absent (remaining length < 1)", so the intent was >= 1 — an upstream
    # off-by-one that hides EVERY reason code this broker sends (0x82, 0x93, 0x94,
    # 0xA1) behind reason 0 "Normal disconnection". Our encoding is spec-legal:
    # §3.14.2.2.1 permits omitting the property length only when Remaining Length < 2,
    # and an explicit zero is always allowed.
    #
    # So this assertion is deliberately NOT Paho's decoder — it is a hand-built v5
    # CONNECT + SUBSCRIBE on a bare socket, which is what keeps the lane able to catch
    # 0xA1 being swapped for 0xA2 (Wildcard Subscriptions not supported).
    expect(
        "the refusal's reason byte is 0xA1 (not 0xA2)",
        "e002a100",
        raw_subscribe_with_identifier(),
    )

    # [MQTT-3.3.4-6]: a client->server PUBLISH MUST NOT carry a Subscription Identifier.
    # This half had NO foreign-client coverage: deleting the guard in conn.rs left the
    # whole lane green, so a real client is what pins it. Paho permits setting the
    # property on a PUBLISH (verified: Properties(PacketTypes.PUBLISH) accepts it), which
    # is exactly the mistake the guard exists to catch.
    pubc = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id="paho-subid-pub", protocol=mqtt.MQTTv5
    )
    pubc.enable_logger(None)
    pseen = {"pubacks": 0}
    pubc.on_publish = lambda cl, u, mid, rc, props: pseen.update(
        pubacks=pseen["pubacks"] + 1
    )
    pubc.on_disconnect = lambda cl, u, flags, rc, props: pseen.update(
        dc=True, from_server=bool(flags.is_disconnect_packet_from_server)
    )
    connect(pubc)
    pub_props = Properties(PacketTypes.PUBLISH)
    pub_props.SubscriptionIdentifier = 5
    pubc.publish("paho/subid-pub", b"x", qos=1, properties=pub_props)
    if not wait(lambda: "dc" in pseen, time.time() + TMO):
        bad(
            "PUBLISH carrying a Subscription Identifier is refused [MQTT-3.3.4-6]",
            "no DISCONNECT arrived",
        )
    else:
        expect(
            "a real client's identifier-bearing PUBLISH is disconnected by the server",
            True,
            pseen.get("from_server"),
        )
        expect("no PUBACK preceded the refusal", 0, pseen["pubacks"])
    pubc.loop_stop()

    # --- 7. Will Delay Interval (0x18): honoured, and cancelled by a resume ----
    #
    # This is the case a foreign oracle FOUND (issue #299): the property round-tripped in
    # the broker's codec with no reader anywhere, and every test the project wrote itself
    # stayed green. Paho puts 0x18 on the wire for us, and the timing is asserted against
    # the wall clock rather than against any broker-internal signal.
    #
    # Both halves need a session that OUTLIVES the disconnect: MQTT 5.0 §3.1.2.11.2 also
    # publishes the will when the session expires, whichever comes first, so a client with
    # SessionExpiryInterval 0 (paho's default) is *supposed* to get its will immediately no
    # matter what delay it asked for.
    watcher = v5_client("paho-will-watch")
    connect(watcher)
    wills = []
    watcher.on_message = lambda cl, u, msg: wills.append((time.time(), msg))
    watcher.subscribe("paho/will/delayed", qos=1)
    watcher.subscribe("paho/will/cancelled", qos=1)
    watcher.subscribe("paho/will/cleanstart", qos=1)
    time.sleep(0.3)

    def dying_client(cid, topic, delay):
        """A v5 client with WillDelayInterval=delay and a 30 s session, killed at the socket."""
        c = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id=cid,
            protocol=mqtt.MQTTv5,
            reconnect_on_failure=False,
        )
        c.enable_logger(None)
        wp = Properties(PacketTypes.WILLMESSAGE)
        wp.WillDelayInterval = delay
        c.will_set(topic, b"gone", qos=1, retain=False, properties=wp)
        cp = Properties(PacketTypes.CONNECT)
        cp.SessionExpiryInterval = 30
        c.connect(HOST, PORT, keepalive=30, clean_start=True, properties=cp)
        c.loop_start()
        time.sleep(0.3)
        return c

    # (a) HONOURED: kill the socket, expect nothing at +1 s and arrival in (2, 5) s.
    dying = dying_client("paho-will-delayed", "paho/will/delayed", 3)
    dying.loop_stop()  # joins the network thread first, which can take up to ~1 s
    dying._sock.close()  # a dead socket, not a DISCONNECT: the will stays armed
    killed_at = time.time()
    time.sleep(1.0)
    delayed = [t for (t, m) in wills if m.topic == "paho/will/delayed"]
    expect("no will one second into a 3 s Will Delay Interval", [], delayed)
    if not wait(
        lambda: any(m.topic == "paho/will/delayed" for (_, m) in wills),
        time.time() + 5.0,
    ):
        bad("a delayed will arrives once its Will Delay Interval elapses")
    else:
        landed = min(t for (t, m) in wills if m.topic == "paho/will/delayed")
        waited = landed - killed_at
        # Upper bound generous on purpose: the broker cannot start the clock before it
        # notices the dead socket, and a loaded CI runner adds to that. The LOWER bound is
        # the conformance assertion (0.1 s was the observed pre-fix value); the upper one
        # only has to exclude "the delay was ignored" and "the will never came".
        if 2.0 < waited < 6.0:
            ok(f"the 3 s delayed will landed after {waited:.2f}s (never early)")
        else:
            bad("delayed will timing", f"landed after {waited:.2f}s, wanted 2-6s")

    # (b) CANCELLED: the same story, but the client comes back inside the window. The will
    # must NEVER be published [MQTT-3.1.3-9] — this is the whole point of the property.
    dying2 = dying_client("paho-will-cancelled", "paho/will/cancelled", 3)
    dying2.loop_stop()
    dying2._sock.close()
    time.sleep(1.0)
    back = mqtt.Client(
        CallbackAPIVersion.VERSION2, client_id="paho-will-cancelled", protocol=mqtt.MQTTv5
    )
    back.enable_logger(None)
    # NO SessionExpiryInterval on the way back in — paho's default, and every v5 client
    # that does not set the property (absent = 0, MQTT 5.0 §3.1.2.11.2). A resume is a
    # resume: [MQTT-3.1.3-9] does not let the resuming CONNECT's own properties decide
    # whether the PREVIOUS connection's will is sent. Re-deriving min(delay, expiry) from
    # this connection made the broker announce the death of a client that had just come
    # back — on the most ordinary reconnect there is.
    back.connect(HOST, PORT, keepalive=30, clean_start=False)
    back.loop_start()
    time.sleep(4.0)  # well past the 3 s the will would have fired at
    expect(
        "a resume with NO Session Expiry is still a resume, never announced dead "
        "[MQTT-3.1.3-9]",
        [],
        [t for (t, m) in wills if m.topic == "paho/will/cancelled"],
    )
    back.loop_stop()
    back.disconnect()

    # (c) DELETED BY A NEW CONNECTION FOR THE CLIENT ID — the shape a foreign oracle is worth
    # having for, because the reading is contested and this client is the ORDINARY one:
    # clean_start=True plus a non-zero Session Expiry Interval is exactly what `dying_client`
    # above sends, i.e. what paho's own examples do.
    #
    # MQTT 5.0 §3.1.2.5: the Will Message MUST be published after the connection closes and
    # either the delay has elapsed or the Session ends, "unless the Will Message has been
    # deleted by the Server on receipt of a DISCONNECT packet with Reason Code 0x00 … or a new
    # Network Connection FOR THE CLIENTID is opened before the Will Delay Interval has
    # elapsed" [MQTT-3.1.2-8]. That exception is keyed on the client id, not on the Session,
    # and it excepts the whole obligation — the "or the Session ends" trigger included. So a
    # Clean Start CONNECT for the same id inside the window deletes the will. An earlier
    # reading here published it (citing [MQTT-3.1.2-4] and "or the Session ends"), which took
    # the feature away from every client that reconnects clean.
    dying3 = dying_client("paho-will-cleanstart", "paho/will/cleanstart", 3)
    dying3.loop_stop()
    dying3._sock.close()
    time.sleep(1.0)
    fresh = mqtt.Client(
        CallbackAPIVersion.VERSION2, client_id="paho-will-cleanstart", protocol=mqtt.MQTTv5
    )
    fresh.enable_logger(None)
    fp = Properties(PacketTypes.CONNECT)
    fp.SessionExpiryInterval = 30
    fresh.connect(HOST, PORT, keepalive=30, clean_start=True, properties=fp)
    fresh.loop_start()
    time.sleep(4.0)  # well past the 3 s the will would have fired at
    expect(
        "a clean-start CONNECT for the same client id inside the window DELETES the will "
        "[MQTT-3.1.2-8]",
        [],
        [t for (t, m) in wills if m.topic == "paho/will/cleanstart"],
    )
    fresh.loop_stop()
    fresh.disconnect()

    watcher.loop_stop()
    watcher.disconnect()

    c.loop_stop()
    c.disconnect()

    print(f"\n  passed: {PASS}   failed: {FAIL}")
    if FAIL:
        print("PAHO INTEROP FAILED")
        sys.exit(1)
    print("PAHO INTEROP OK")


if __name__ == "__main__":
    main()
