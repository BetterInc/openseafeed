#!/usr/bin/env python3
"""End-to-end smoke test.

Requires ingest (:10110/udp, :8080), pipeline, fanout (:8081) and
snapshotter (:8082) running against one NATS server, all with
OSF_KEYS_MODE=dev. Feeds testdata/replay.nmea via UDP, then asserts:

1. an aisstream.io-style WebSocket subscription over Seattle receives the
   PositionReport for MMSI 477553000;
2. the full-fleet snapshot contains that vessel;
3. an invalid API key is rejected with an error frame;
4. a fresh subscription is warmed from the snapshot (initial state) and
   `"InitialState": false` opts out of it.

deps: pip install websockets requests
"""

import asyncio
import gzip
import json
import pathlib
import socket
import sys
import time

import requests
import websockets

ROOT = pathlib.Path(__file__).resolve().parent.parent
REPLAY = ROOT / "testdata" / "replay.nmea"
UDP_ADDR = ("127.0.0.1", 10110)
FANOUT = "ws://127.0.0.1:8081/v1/stream"
SNAPSHOT = "http://127.0.0.1:8082/v1/snapshot"
KEY = "osf_live_e2etest12345"

# Seattle-ish bbox in aisstream format: two [lat, lon] corners.
SUBSCRIBE = {
    "APIKey": KEY,
    "BoundingBoxes": [[[47.0, -123.0], [48.0, -122.0]]],
}


def feed_udp(repeat: int = 3, delay: float = 0.3) -> None:
    lines = REPLAY.read_text().strip().splitlines()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for _ in range(repeat):
        for line in lines:
            sock.sendto((line + "\r\n").encode(), UDP_ADDR)
        time.sleep(delay)


async def test_stream() -> dict:
    async with websockets.connect(FANOUT) as ws:
        await ws.send(json.dumps(SUBSCRIBE))
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, feed_udp)
        deadline = time.time() + 20
        while time.time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
            msg = json.loads(raw)
            if "error" in msg:
                raise AssertionError(f"stream error: {msg}")
            if msg["MetaData"]["MMSI"] == 477553000:
                assert msg["MessageType"] == "PositionReport", msg
                report = msg["Message"]["PositionReport"]
                assert abs(report["Latitude"] - 47.582833) < 1e-4, report
                assert report["NavigationalStatus"] == 5, report
                return msg
        raise AssertionError("did not receive MMSI 477553000 within 20s")


async def test_bad_key() -> None:
    async with websockets.connect(FANOUT) as ws:
        await ws.send(json.dumps({"APIKey": "bogus", "BoundingBoxes": [[[0, 0], [1, 1]]]}))
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        assert "error" in json.loads(raw), raw


def test_snapshot() -> None:
    # Snapshots are generated on an interval; poll briefly.
    deadline = time.time() + 90
    while time.time() < deadline:
        r = requests.get(SNAPSHOT, params={"key": KEY}, timeout=5)
        if r.status_code == 200:
            body = r.content
            data = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
            mmsis = {v["mmsi"] for v in data["vessels"]}
            if 477553000 in mmsis:
                print(f"snapshot ok: {data['count']} vessels")
                return
        time.sleep(3)
    raise AssertionError("snapshot never contained the test vessel")


async def test_warm_start() -> None:
    """A new subscriber gets the fleet we already hold, with no new traffic.

    Nothing is fed during this test: every frame received has to come from
    fan-out's seed of the snapshotter's state.
    """
    async with websockets.connect(FANOUT) as ws:
        await ws.send(json.dumps(SUBSCRIBE))
        deadline = time.time() + 30
        while time.time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
            msg = json.loads(raw)
            if "error" in msg:
                raise AssertionError(f"stream error: {msg}")
            if msg["MetaData"]["MMSI"] == 477553000:
                assert msg["MessageType"] == "PositionReport", msg
                # The replayed frame carries the vessel's own last-heard time,
                # not the moment of replay.
                assert msg["MetaData"]["time_utc"], msg
                return
        raise AssertionError("no initial state for MMSI 477553000 within 30s")


async def test_warm_start_opt_out() -> None:
    """`InitialState: false` must not replay the vessel the seed holds.

    Other vessels may still arrive here from whatever upstream feeds the
    stack has connected - only the replay vessel, which is not transmitting,
    proves whether the seed was sent.
    """
    async with websockets.connect(FANOUT) as ws:
        await ws.send(json.dumps({**SUBSCRIBE, "InitialState": False}))
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
            except asyncio.TimeoutError:
                return  # silence is the point
            msg = json.loads(raw)
            if "error" in msg:
                raise AssertionError(f"stream error: {msg}")
            if msg["MetaData"]["MMSI"] == 477553000:
                raise AssertionError(f"expected no initial state, got {msg}")


def main() -> None:
    r = requests.get(SNAPSHOT.replace("/v1/snapshot", "/healthz"), timeout=5)
    assert r.ok, "snapshotter not healthy"

    msg = asyncio.run(test_stream())
    print(f"stream ok: got {msg['MessageType']} for {msg['MetaData']['MMSI']}")
    asyncio.run(test_bad_key())
    print("bad key rejected ok")
    test_snapshot()
    # The seed is refreshed on its own interval; give fan-out one cycle to
    # pick the snapshot up (OSF_SEED_REFRESH_SECS is 15 in the dev stack).
    time.sleep(20)
    asyncio.run(test_warm_start())
    print("warm start ok: new subscriber seeded from the snapshot")
    asyncio.run(test_warm_start_opt_out())
    print("InitialState=false opts out ok")
    print("E2E PASS")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"E2E FAIL: {e}", file=sys.stderr)
        sys.exit(1)
