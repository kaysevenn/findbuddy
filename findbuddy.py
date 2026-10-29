import collections
import json
import math
import subprocess
import sys
import threading
import time

import paho.mqtt.client as mqtt

from config import ARRIVE_M, HOST, PORT, ROUND_DIGITS, TOPIC

EARTH_R = 6371000.0
HEADING_WINDOW = 8
POSITION_WINDOW = 3
GPS_EVERY = 3.0
STALE_SECONDS = 60.0
PRINT_EVERY = 1.0
SIM_ME = (-7.0500, 110.4380)

args = sys.argv[1:]
positional = [a for a in args if not a.startswith("--")]
device_id = positional[0] if positional else "edge1"
simulate = "--sim" in args
sim_started = time.monotonic()

lock = threading.Lock()
peer_seen = {"at": 0.0}


class Track:
    def __init__(self):
        self.points = collections.deque(maxlen=POSITION_WINDOW)

    def add(self, lat, lon):
        self.points.append((lat, lon))

    def value(self):
        if not self.points:
            return None
        n = len(self.points)
        return sum(p[0] for p in self.points) / n, sum(p[1] for p in self.points) / n


my_track = Track()
peer_track = Track()


def haversine(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R * math.asin(math.sqrt(a))


def bearing(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return math.degrees(math.atan2(y, x)) % 360


def relative_angle(target, heading):
    return (target - heading + 540) % 360 - 180


def circular_mean(angles):
    s = sum(math.sin(math.radians(a)) for a in angles)
    c = sum(math.cos(math.radians(a)) for a in angles)
    return math.degrees(math.atan2(s, c)) % 360


def format_distance(meters):
    return f"{meters / 1000:.1f} km" if meters >= 1000 else f"{meters:.0f} m"


def guidance(rel, dist):
    if dist <= ARRIVE_M:
        return "SAMPAI"
    if abs(rel) <= 15:
        return "LURUS"
    side = "kanan" if rel > 0 else "kiri"
    return f"Putar {side} {abs(rel):.0f}°"


def read_location():
    for provider, wait in (("gps", 30), ("network", 15)):
        try:
            out = subprocess.run(
                ["termux-location", "-p", provider, "-r", "once"],
                capture_output=True,
                text=True,
                timeout=wait,
                stdin=subprocess.DEVNULL,
            ).stdout
            data = json.loads(out)
            return data["latitude"], data["longitude"], data.get("accuracy", 0.0)
        except (subprocess.TimeoutExpired, json.JSONDecodeError, KeyError):
            continue
    return None


def sim_location():
    return SIM_ME[0], SIM_ME[1], 5.0


def sim_peer():
    t = time.monotonic() - sim_started
    d = max(0.0, 300.0 - 3.0 * t)
    north = d * math.cos(math.radians(45))
    east = d * math.sin(math.radians(45))
    return SIM_ME[0] + north / 111320.0, SIM_ME[1] + east / 110500.0


def magnetometer_name():
    out = subprocess.run(
        ["termux-sensor", "-l"],
        capture_output=True,
        text=True,
        check=True,
        stdin=subprocess.DEVNULL,
    ).stdout
    names = [
        n
        for n in json.loads(out)["sensors"]
        if "magnetometer" in n.lower() and "uncalibrated" not in n.lower()
    ]
    names.sort(key=lambda n: not n.lower().endswith("non-wakeup"))
    return names[0]


def read_stream(name):
    proc = subprocess.Popen(
        ["termux-sensor", "-s", name, "-d", "200"],
        stdout=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
    )
    decoder = json.JSONDecoder()
    buffer = ""
    for line in proc.stdout:
        buffer += line
        try:
            obj, _ = decoder.raw_decode(buffer.lstrip())
        except json.JSONDecodeError:
            continue
        buffer = ""
        yield obj


def heading_source():
    if simulate:
        while True:
            yield (time.monotonic() - sim_started) * 15 % 360
            time.sleep(0.2)
    else:
        name = magnetometer_name()
        for obj in read_stream(name):
            for payload in obj.values():
                mx, my = payload["values"][0], payload["values"][1]
                yield math.degrees(math.atan2(-mx, my)) % 360


def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe(f"{TOPIC}/+/position", qos=0)
    print(f"[{device_id}] terhubung ke broker")


def on_message(client, userdata, msg):
    if simulate:
        return
    data = json.loads(msg.payload)
    if data["device_id"] == device_id:
        return
    with lock:
        peer_track.add(data["lat"], data["lon"])
        peer_seen["at"] = time.monotonic()


def position_loop(client):
    sequence = 0
    while True:
        started = time.monotonic()
        loc = sim_location() if simulate else read_location()
        if loc:
            lat, lon, accuracy = loc
            with lock:
                my_track.add(lat, lon)
            sequence += 1
            payload = {
                "device_id": device_id,
                "sequence": sequence,
                "lat": round(lat, ROUND_DIGITS),
                "lon": round(lon, ROUND_DIGITS),
                "accuracy": round(accuracy, 1),
                "sent_at_ms": int(time.time() * 1000),
            }
            client.publish(f"{TOPIC}/{device_id}/position", json.dumps(payload), qos=0)
        time.sleep(max(0.0, GPS_EVERY - (time.monotonic() - started)))


def report(heading):
    with lock:
        if simulate:
            peer_track.add(*sim_peer())
            peer_seen["at"] = time.monotonic()
        me = my_track.value()
        peer = peer_track.value()
        age = time.monotonic() - peer_seen["at"]
    if me is None:
        print("Menunggu GPS sendiri...")
        return
    if peer is None or age > STALE_SECONDS:
        print("Menunggu posisi teman...")
        return
    dist = haversine(me[0], me[1], peer[0], peer[1])
    target = bearing(me[0], me[1], peer[0], peer[1])
    rel = relative_angle(target, heading)
    print(
        f"hadap={heading:.0f}° target={target:.0f}° "
        f"jarak={format_distance(dist)} | {guidance(rel, dist)}"
    )


def main():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"findbuddy-{device_id}-{int(time.time())}",
    )
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(HOST, PORT, keepalive=30)
    client.loop_start()
    threading.Thread(target=position_loop, args=(client,), daemon=True).start()

    headings = collections.deque(maxlen=HEADING_WINDOW)
    last_print = 0.0
    for h in heading_source():
        headings.append(h)
        now = time.monotonic()
        if now - last_print < PRINT_EVERY:
            continue
        last_print = now
        report(circular_mean(headings))


if __name__ == "__main__":
    main()