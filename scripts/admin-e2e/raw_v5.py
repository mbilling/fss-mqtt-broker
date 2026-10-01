"""A minimal MQTT 5 client for scripts/admin-e2e: CONNECT (optionally with a username and
password, and a persistent session), optionally SUBSCRIBE, then print every packet the
server sends until it closes. Unlike mosquitto_sub it never reconnects, so a kick, an
eviction or a takeover is observable.

Usage: raw_v5.py <port> <client-id> [--user U --password P] [--sub FILTER] [--persist]
       [--timeout SECONDS]
Prints: CONNACK reason=0xNN · SUBACK reasons=… · PUBLISH <topic> <payload> ·
        DISCONNECT reason=0xNN · server closed the connection · timeout
"""
import argparse
import socket
import struct
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("port", type=int)
ap.add_argument("cid")
ap.add_argument("--user")
ap.add_argument("--password")
ap.add_argument("--sub")
ap.add_argument("--persist", action="store_true")
ap.add_argument("--timeout", type=float, default=60)
args = ap.parse_args()


def s(b):
    return struct.pack("!H", len(b)) + b


def varint(n):
    out = b""
    while True:
        byte = n % 128
        n //= 128
        out += bytes([byte | (0x80 if n else 0)])
        if not n:
            return out


def packet(first, body):
    return bytes([first]) + varint(len(body)) + body


flags = 0x00 if args.persist else 0x02  # clean start unless persistent
props = b"\x11\xff\xff\xff\xff" if args.persist else b""  # session expiry: never
if args.user:
    flags |= 0x80
if args.password:
    flags |= 0x40
var = s(b"MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + varint(len(props)) + props
payload = s(args.cid.encode())
if args.user:
    payload += s(args.user.encode())
if args.password:
    payload += s(args.password.encode())
sock = socket.create_connection(("127.0.0.1", args.port))
sock.sendall(packet(0x10, var + payload))
sock.settimeout(args.timeout)
buf = b""
start = time.time()


def emit(line):
    print(line)
    sys.stdout.flush()


def take():
    """One complete packet from buf: (type, flags, body), or None."""
    global buf
    if len(buf) < 2:
        return None
    mult, length, i = 1, 0, 1
    while True:
        if i >= len(buf):
            return None
        b = buf[i]
        length += (b & 0x7F) * mult
        mult *= 128
        i += 1
        if not b & 0x80:
            break
    if len(buf) < i + length:
        return None
    first, body = buf[0], buf[i:i + length]
    buf = buf[i + length:]
    return first >> 4, first & 0x0F, body


while True:
    try:
        chunk = sock.recv(4096)
    except socket.timeout:
        emit("timeout")
        break
    except ConnectionError:
        emit("server closed the connection")
        break
    if not chunk:
        emit("server closed the connection")
        break
    buf += chunk
    while (pkt := take()) is not None:
        ptype, pflags, body = pkt
        if ptype == 2:
            emit(f"CONNACK reason=0x{body[1]:02x}")
            if body[1] == 0 and args.sub:
                sub = struct.pack("!H", 1) + b"\x00" + s(args.sub.encode()) + b"\x01"
                sock.sendall(packet(0x82, sub))
        elif ptype == 9:
            emit("SUBACK reasons=" + ",".join(f"0x{b:02x}" for b in body[3:]))
        elif ptype == 3:
            qos = (pflags >> 1) & 3
            tlen = struct.unpack("!H", body[:2])[0]
            topic = body[2:2 + tlen].decode()
            i = 2 + tlen
            pkid = None
            if qos:
                pkid = struct.unpack("!H", body[i:i + 2])[0]
                i += 2
            plen, mult = 0, 1
            while True:
                b = body[i]
                plen += (b & 0x7F) * mult
                mult *= 128
                i += 1
                if not b & 0x80:
                    break
            i += plen
            emit(f"PUBLISH {topic} {body[i:].decode(errors='replace')}")
            if qos == 1:
                sock.sendall(packet(0x40, struct.pack("!H", pkid)))
        elif ptype == 14:
            emit(f"DISCONNECT reason=0x{body[0]:02x} after {time.time() - start:.1f}s")
        else:
            emit(f"packet type {ptype}")
