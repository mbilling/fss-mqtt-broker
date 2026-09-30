"""A minimal MQTT 5 client for scripts/admin-e2e: CONNECT, then print whatever the server
sends until it closes. Unlike mosquitto_sub it never reconnects, so a kick is observable.
Usage: raw_v5.py <port> <client-id>"""
import socket, struct, sys, time

port, cid = int(sys.argv[1]), sys.argv[2].encode()

def s(b):
    return struct.pack("!H", len(b)) + b

var = s(b"MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + b"\x00"  # clean start, no props
payload = s(cid)
body = var + payload
pkt = bytes([0x10, len(body)]) + body
sock = socket.create_connection(("127.0.0.1", port))
sock.sendall(pkt)
sock.settimeout(20)
buf = b""
start = time.time()
while True:
    try:
        chunk = sock.recv(1024)
    except socket.timeout:
        print("timeout"); break
    if not chunk:
        print("server closed the connection"); break
    buf += chunk
    while len(buf) >= 2:
        ptype, length = buf[0] >> 4, buf[1]
        if len(buf) < 2 + length:
            break
        body, buf = buf[2:2 + length], buf[2 + length:]
        if ptype == 2:
            print(f"CONNACK reason=0x{body[1]:02x}"); sys.stdout.flush()
        elif ptype == 14:
            print(f"DISCONNECT reason=0x{body[0]:02x} after {time.time()-start:.1f}s"); sys.stdout.flush()
        else:
            print(f"packet type {ptype}")
