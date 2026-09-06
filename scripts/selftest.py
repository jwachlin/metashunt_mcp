#!/usr/bin/env python3
"""Self-test for the MetaShunt V2 capture pipeline.

Walks the connection/measurement path against a *real* device:

    1. Device discovery  : find the USB serial port by VID/PID
    2. Connection        : open the port via the driver
    3. Continuous stream : record ~N seconds, verify samples arrive
    4. Storage           : confirm a session was written to disk
    5. Burst (optional)  : issue a device burst read and count samples

Pure-python STDLIB only (beyond the package itself) so it runs under
``uv run scripts/selftest.py`` without extra tooling.

Exit code is 0 on success, 1 on any failed stage.  Run offline (no device) by
setting ``METASHUNT_SELFTEST_SIMULATE=1`` to sanity-check the plumbing against
a fake device before touching hardware.

Metrics/units:
  * current is reported in mA by the device and stored raw.
"""

import argparse
import os
import sys
import time

# Ensure the repo root is importable so `tests.fake_serial` (used for the
# offline `--simulate` mode) resolves regardless of the CWD the script is run
# from.  Running under `uv run` this also matches the installed package.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _inline_deps():
    """Guard: this script imports the package lazily so a helpful message is
    printed if deps are missing, instead of a raw ImportError stack."""
    try:
        import numpy  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        sys.stderr.write(
            "Run inside the project environment:  uv run scripts/selftest.py\n"
            f"  (missing dependency: {exc.name})\n"
        )
        raise SystemExit(2) from None


_inline_deps()

import serial.tools.list_ports  # noqa: E402

import numpy as np  # noqa: E402

from metashunt_mcp import config  # noqa: E402
from metashunt_mcp.capture import CaptureEngine, KIND_BURST  # noqa: E402
from metashunt_mcp.store import SessionStore  # noqa: E402


def find_device():
    for comport in serial.tools.list_ports.comports():
        if comport.vid == config.METASHUNT_VID and comport.pid == config.METASHUNT_PID:
            return comport.device
    return None


def sample_rate_hz(t_us, lo_pct=10, hi_pct=90):
    """Estimate the sample rate in Hz from a monotonic us time base.

    Uses the median of the interior per-sample deltas (tenth..ninetieth
    percentile) so a few dropped/garbled samples don't skew the estimate.
    ``t_us`` is the unwrapped device-time array in microseconds; dt is the per
    sample interval, so rate = 1e6 / median(dt).
    """
    if len(t_us) < 4:
        return None
    dt = np.diff(t_us)
    lo = np.percentile(dt, lo_pct)
    hi = np.percentile(dt, hi_pct)
    interior = dt[(dt >= lo) & (dt <= hi) & (dt > 0)]
    if len(interior) == 0:
        return None
    return 1e6 / np.median(interior)


def run_selftest(simulate: bool, duration: float, do_burst: bool, rate_hz: int,
                 log_dir):
    print("=" * 60)
    print("MetaShunt V2 self-test")
    print("=" * 60)

    manager = None

    try:
        # ---- 1. device discovery ------------------------------------------
        print(f"\n[1/5] Device discovery  (VID {config.METASHUNT_VID}:"
              f"{config.METASHUNT_PID})")
        port = find_device() if not simulate else "_simulated_"
        if simulate:
            print("      SIMULATE: skipping real discovery")
        if not port:
            if simulate:
                print("      (continuing with injected fake)")
            else:
                print("      FAIL: no MetaShunt found on the serial bus")
                return 1
        else:
            print(f"      found on port: {port}")

        # ---- 2. connection ------------------------------------------------
        print("\n[2/5] Connecting")
        if simulate:
            # Reuse the exact seam the unit tests use: drive a fake device so
            # we can validate the whole capture engine offline.
            from tests.fake_serial import FakeSerial, make_stream, packets_for  # noqa: E402
            from metashunt_mcp.metashunt_v2_lib import MetaShuntV2  # noqa: E402
            drv = MetaShuntV2()
            _fs = FakeSerial()
            drv.ser = _fs  # attach fake serial directly (bypasses discovery)
            _fs.feed(packets_for(make_stream(n=40000)))
            manager = CaptureEngine(store=SessionStore(log_dir=log_dir),
                                    driver=drv, ring_size=1 << 16)
            manager.driver._ser_ref = _fs  # keep for later injection
        else:
            manager = CaptureEngine(store=SessionStore(log_dir=log_dir))
            if not manager.driver.connect(port):
                print("      FAIL: could not open serial port")
                return 1
            print("      connected OK")

        # ---- 3. continuous stream ----------------------------------------
        print(f"\n[3/5] Streaming ~{duration}s")
        manager.start()
        session = manager.start_stream(meta={"label": "selftest"})
        print(f"      recording to session: {session}")

        deadline = time.time() + duration
        n = 0
        while time.time() < deadline:
            time.sleep(0.25)
            n, mean, _std = manager.ring_stats()
            if simulate:
                # keep injecting so the fake device behaves like a live stream
                manager.driver._ser_ref.feed(packets_for(make_stream(n=40000)))
            print(f"\r      {n} samples so far", end="", flush=True)
        print()

        n, mean, std = manager.ring_stats()
        last = manager.ring_last()
        print(f"      total ring samples: {n}")
        if n == 0:
            print("      FAIL: no measurements received during the stream window")
            return 1
        # pull interior stream samples for rate estimation (skip the ring's
        # oldest burst-tagged entries so we measure the continuous stream)
        t_ring, _c_ring, k_ring = manager.ring_after(max(0, manager.ring_head() - manager._count))
        stream_mask = k_ring == 0  # KIND_STREAM
        if stream_mask.any():
            rate = sample_rate_hz(t_ring[stream_mask])
            if rate is None:
                print("      WARN: too few samples to estimate stream rate")
            else:
                ok = 5500.0 <= rate <= 7500.0
                print(f"      stream rate: {rate:.0f} Hz  "
                      f"({'OK' if ok else 'FAIL'}) expected 5500-7500 Hz")
                if not ok:
                    return 1
        print(f"      mean current: {mean:.6f} mA   std: {std:.6f} mA")
        print(f"      last sample : {last}")

        # ---- 4. storage ---------------------------------------------------
        # Flush the active session by stopping it (as the MCP stop_capture
        # flow does).  Buffered samples only reach disk on stop / every
        # 20,000 samples / every 5 s, so checking before a flush would
        # reliably show 0 on short live runs.
        print("\n[4/5] Storage")
        manager.stop_stream()
        stored = manager.store.count(session)
        print(f"      session '{session}' on disk: {stored} samples")
        if stored == 0:
            print("      FAIL: session was not flushed to disk")
            return 1
        print("      storage OK")

        # ---- 5. burst (optional) -----------------------------------------
        if do_burst:
            print(f"\n[5/5] Burst read  (rate {rate_hz} Hz)")
            # In both simulate and live mode the burst response arrives after the
            # request is sent.  In simulate mode the FakeSerial auto-generates the
            # 37,500 samples at the requested rate (parsed from rate500 in the
            # command), so no manual feed is needed here.
            boundary = manager.ring_head()
            ok = manager.request_burst(rate_hz=rate_hz, trig_id=0, level=0)
            t, c, k = manager.ring_after(boundary)
            burst_n = int((k == KIND_BURST).sum())
            print(f"      completed: {ok}  burst samples: {burst_n}")
            if not ok or burst_n == 0:
                print("      FAIL: burst did not complete / return data")
                return 1
            # Validate the achieved burst rate vs the request.
            # The device rounds the requested rate up to 500 Hz steps and the
            # measured rate typically exceeds the request, so require only that
            # the burst came back at least as fast as requested (no upper end).
            burst_mask = k == KIND_BURST
            b_rate = sample_rate_hz(t[burst_mask])
            if b_rate is None:
                print("      WARN: could not estimate burst rate")
            else:
                okr = b_rate >= rate_hz
                print(f"      burst rate: {b_rate:.0f} Hz  ({'OK' if okr else 'FAIL'})"
                      f" expected >= {rate_hz} Hz for {rate_hz} Hz request")
                if not okr:
                    print(f"      FAIL: burst rate {b_rate:.0f} Hz slower than "
                          f"the {rate_hz} Hz request")
                    return 1
            print("      burst OK")
        else:
            print("\n[5/5] Burst skipped  (pass --burst to enable)")

        print("\n" + "=" * 60)
        print("SELF-TEST PASSED")
        print("=" * 60)
        return 0

    finally:
        if manager is not None:
            try:
                manager.dispose()
            except Exception:  # pragma: no cover - cleanup best-effort
                pass


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--simulate", action="store_true",
                    help="run against an injected fake device (offline)")
    ap.add_argument("-t", "--duration", type=float, default=3.0,
                    help="streaming duration in seconds (default 3.0)")
    ap.add_argument("--burst", action="store_true",
                    help="also issue a device burst read")
    ap.add_argument("--rate", type=int, default=100000,
                    help="burst rate in Hz when --burst is set (default 100000)")
    ap.add_argument("--log-dir", default="/tmp/msx_selftest",
                    help="directory for the self-test session (default /tmp/msx_selftest)")
    args = ap.parse_args()

    # honour the env override used by the CI / hardware gates
    simulate = (args.simulate
                or os.environ.get("METASHUNT_SELFTEST_SIMULATE", "0") == "1")
    rc = run_selftest(
        simulate=simulate,
        duration=args.duration,
        do_burst=args.burst,
        rate_hz=args.rate,
        log_dir=args.log_dir,
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()