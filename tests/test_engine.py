import threading
import time

import numpy as np

from metashunt_mcp.capture import CaptureEngine, KIND_STREAM, KIND_BURST
from metashunt_mcp.metashunt_v2_lib import MetaShuntV2
from metashunt_mcp.store import SessionStore

from fake_serial import FakeSerial, make_stream, packets_for


def _fake_driver():
    drv = MetaShuntV2()
    fs = FakeSerial()
    drv.ser = fs
    return drv, fs


def test_streaming_records_session(tmp_path):
    drv, fs = _fake_driver()
    eng = CaptureEngine(
        store=SessionStore(log_dir=tmp_path),
        driver=drv,
        ring_size=4096,
    )
    eng.start()
    eng.start_stream("test_session")
    fs.feed(packets_for(make_stream()))
    deadline = time.time() + 5.0
    while time.time() < deadline:
        sess = [s for s in eng.store.list_sessions() if s["name"] == "test_session"]
        if sess and sess[0]["samples"] >= 20000:
            break
        time.sleep(0.05)
    assert sess and sess[0]["samples"] == 20000

    t, c, k = eng.ring_tail(eng._count)
    assert t.shape[0] == 4096, t.shape[0]
    assert (k == KIND_STREAM).all()

    n, mean, _std = eng.ring_stats()
    assert n == 4096
    assert 0.09 < mean < 0.3, mean

    eng.stop_stream()
    eng._flush_session()
    eng.dispose()

    t2, c2, k2 = eng.store.load("test_session")
    assert t2.shape[0] == 20000
    assert c2.max() > 3.9, "spike should be in the stored log"
    assert (k2 == b"stream").all()
    print("ok streaming/session")


def test_start_request_burst(tmp_path):
    drv, fs = _fake_driver()
    eng = CaptureEngine(
        store=SessionStore(log_dir=tmp_path),
        driver=drv,
        ring_size=1024 * 64,
    )
    eng.start()
    eng.start_stream("before_burst")
    ok = eng.request_burst(rate_hz=1000, trig_id=0, level=0)
    assert ok, "burst should complete"
    assert eng.state() == "STREAMING", "device auto-returns to streaming"

    t, c, k = eng.ring_tail(eng._count)
    burst_n = int((k == KIND_BURST).sum())
    assert burst_n == 37500, burst_n
    assert fs.writes[0][0] == 0xAA and fs.writes[0][3] == 2  # 1000Hz/500
    eng.stop_stream()
    eng._flush_session()
    eng.dispose()

    t2, c2, k2 = eng.store.load("before_burst")
    assert (k2 == b"burst").sum() == 37500
    print("ok burst fsm")