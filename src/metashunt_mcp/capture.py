"""Background capture engine for MetaShunt V2.

Owns the single serial reader thread and a finite-state machine over it::

    IDLE ---start_stream()---> STREAMING ---stop_stream()---> IDLE
    IDLE ---request_burst()--> BURST ---(37,500 samples)---> STREAMING

The device always emits samples; in IDLE we keep reading but discard them (so
the link stays healthy and we can observe the device), in STREAMING/BURST we
record them to an in-memory ring buffer and the active session.

All device-side time is the monotonic unwrapped us base.  Wall-clock is only
used to schedule log flushes and to timestamp session metadata.
"""

import threading
import time

import numpy as np

from metashunt_mcp import config
from metashunt_mcp.metashunt_v2_lib import (
    BURST_NUM_SAMPLES,
    MetaShuntV2,
    TickUnwrapper,
)
from metashunt_mcp.store import SessionStore
from metashunt_mcp.triggers import TriggerManager

# kind tags for the ring buffer / session logs
KIND_STREAM = 0
KIND_BURST = 1


class CaptureEngine:
    def __init__(self, store=None, driver=None, ring_size=None):
        self.driver = driver if driver is not None else MetaShuntV2()
        self.store = store if store is not None else SessionStore()
        self.triggers = TriggerManager()
        self._unwrap = TickUnwrapper()

        ring_size = ring_size or config.RING_BUFFER_SIZE
        self._cap = int(ring_size)
        self._t = np.empty(self._cap, dtype=np.float64)
        self._c = np.empty(self._cap, dtype=np.float32)
        self._k = np.empty(self._cap, dtype=np.uint8)
        self._head = 0          # next write index
        self._count = 0         # number of valid samples in ring

        self._lock = threading.RLock()
        self._state = "IDLE"
        self._stop = threading.Event()
        self._thread = None
        self._burst_remaining = 0
        self._reader_error = None

        # active session accumulation (flushed to store periodically)
        self._session_name = None
        self._sess_t = []
        self._sess_c = []
        self._sess_k = []
        self._last_flush = time.monotonic()
        self._session_started_wall = None
        self._recv_lock = threading.Condition()

    # ------------------------------------------------------------------ #
    # ring buffer helpers
    # ------------------------------------------------------------------ #

    def _append(self, t_us, cur_ma, kind):
        with self._lock:
            idx = self._head % self._cap
            self._t[idx] = t_us
            self._c[idx] = cur_ma
            self._k[idx] = kind
            self._head += 1
            if self._count < self._cap:
                self._count += 1

    def _ring_indexes(self, n):
        if n >= self._count:
            start = 0
            cnt = self._count
        else:
            start = (self._head - n) % self._cap
            cnt = n
        idx = np.arange(start, start + cnt) % self._cap
        return self._t[idx], self._c[idx], self._k[idx]

    def ring_head(self):
        """Return the current logical write position (for snapshots)."""
        with self._lock:
            return self._head

    def ring_after(self, head0):
        """Return ``(t_us, cur_ma, kind)`` for samples written at/after the
        logical position ``head0`` (as returned by :meth:`ring_head`)."""
        with self._lock:
            n = self._head - head0
            if n <= 0:
                return (np.empty(0), np.empty(0, dtype=np.float32),
                        np.empty(0, dtype=np.uint8))
            if n > self._cap:
                n = self._cap  # ring overwrote the oldest of the window
            return self._ring_indexes(n)

    def ring_tail(self, n):
        """Return ``(t_us, cur_ma, kind)`` for the last ``n`` raw samples."""
        with self._lock:
            return self._ring_indexes(n)

    def ring_stats(self, n=None):
        with self._lock:
            _, c, _ = self._ring_indexes(n if n else self._count)
            if c.shape[0] == 0:
                return 0, None, None
            return (int(c.shape[0]), float(np.mean(c)), float(np.std(c)))

    def ring_last(self):
        with self._lock:
            if self._count == 0:
                return None
            idx = (self._head - 1) % self._cap
            return float(self._t[idx]), float(self._c[idx])

    # ------------------------------------------------------------------ #
    # session recording
    # ------------------------------------------------------------------ #

    def _begin_session(self, name, meta=None):
        with self._lock:
            self._session_name = name
            self._sess_t = []
            self._sess_c = []
            self._sess_k = []
            self._session_started_wall = time.time()
        base = {"started_wall": time.time(), "state": "stream"}
        if meta:
            base.update(meta)
        self.store.update_meta(name, base)

    def _record(self, t_us, cur_ma, kind):
        if self._session_name is None:
            return
        self._sess_t.append(t_us)
        self._sess_c.append(cur_ma)
        self._sess_k.append(kind)
        if len(self._sess_t) >= 20000 or (time.monotonic() - self._last_flush) >= 5.0:
            self._flush_session()

    def _flush_session(self):
        if self._session_name is None or not self._sess_t:
            return
        t = np.asarray(self._sess_t, dtype=np.float64)
        c = np.asarray(self._sess_c, dtype=np.float32)
        k = np.asarray(self._sess_k, dtype=np.uint8)
        # segment stream vs burst kinds into their own npz append calls
        mask = k == KIND_STREAM
        if mask.any():
            self.store.append(self._session_name, t[mask], c[mask], kind="stream")
        mask = k == KIND_BURST
        if mask.any():
            self.store.append(self._session_name, t[mask], c[mask], kind="burst")
        self._sess_t = []
        self._sess_c = []
        self._sess_k = []
        self._last_flush = time.monotonic()

    # ------------------------------------------------------------------ #
    # reader thread / FSM
    # ------------------------------------------------------------------ #

    def _reader_loop(self):
        try:
            self._reader_loop_body()
        except Exception as exc:  # noqa: BLE001 - reader must never die silently
            with self._lock:
                self._reader_error = exc
            try:
                self._flush_session()
            except Exception:
                pass
            with self._recv_lock:
                self._recv_lock.notify_all()

    def _reader_loop_body(self):
        while not self._stop.is_set():
            ms = self.driver.read_one(timeout=config.SERIAL_READ_TIMEOUT)
            if ms is None:
                continue

            t_us = self._unwrap.feed(ms.time)
            with self._lock:
                state = self._state

            if state == "IDLE":
                with self._lock:
                    self._append(t_us, ms.current_ma, KIND_STREAM)
                continue

            if state == "BURST":
                self._append(t_us, ms.current_ma, KIND_BURST)
                self._record(t_us, ms.current_ma, KIND_BURST)
                with self._lock:
                    self._burst_remaining -= 1
                    if self._burst_remaining <= 0:
                        self._state = "STREAMING"
                if self._burst_remaining <= 0:
                    with self._recv_lock:
                        self._recv_lock.notify_all()
                continue

            # STREAMING
            self._append(t_us, ms.current_ma, KIND_STREAM)
            self._record(t_us, ms.current_ma, KIND_STREAM)
            fired = self.triggers.feed(t_us, ms.current_ma)
            if fired:
                for name, _, ev_t, ev_c in fired:
                    self._flush_session()
                    self.store.update_meta(self._session_name, {
                        "last_event": {"trigger": name, "t_us": ev_t, "cur_ma": ev_c,
                                       "wall": time.time()}
                    })
        # thread exit
        self._flush_session()

    # public control (callable from MCP worker threads) ------------------

    def start(self):
        if not self.driver.connected:
            if not self.driver.connect():
                raise RuntimeError("could not connect to MetaShunt V2")
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._reader_loop,
                                        name="metashunt-reader", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self._thread = None

    def start_stream(self, session_name=None, meta=None):
        with self._lock:
            if self._state == "STREAMING":
                return self._session_name
        name = session_name or self.store.new_session_name()
        self._begin_session(name, meta=meta)
        with self._lock:
            self._state = "STREAMING"
        return name

    def stop_stream(self):
        """Stop listening / recording but leave the reader thread alive at idle.

        The MetaShunt itself keeps measuring on its own (no stop command
        exists); the next samples simply get discarded until a new session
        starts.
        """
        with self._lock:
            self._state = "IDLE"
            self._burst_remaining = 0
        self._flush_session()
        name = self._session_name
        if name:
            self.store.update_meta(name, {"ended_wall": time.time()})
        self._session_name = None

    def _raise_reader_error(self):
        with self._lock:
            err = self._reader_error
        if err is not None:
            raise RuntimeError(f"capture reader failed: {err}") from err
        if self._thread is None or not self._thread.is_alive():
            raise RuntimeError("capture reader thread is not running")

    def request_burst(self, rate_hz, trig_id, level=0, timeout=30.0):
        """Command a device burst read; returns when fully received.

        Trigger ids: 0 immediate, 1 rising(uA), 2 falling(uA), 3 stage,
        4 KEY2.  If not currently streaming, transiently records the burst into
        a fresh session; otherwise the burst is appended to the active session.
        """
        self._raise_reader_error()
        if not self.driver.connected:
            raise RuntimeError("MetaShunt not connected")
        with self._lock:
            was_streaming = self._state == "STREAMING"
            self.driver.send_burst(rate_hz, trig_id, level)
            self._state = "BURST"
            self._burst_remaining = BURST_NUM_SAMPLES

        if not was_streaming:
            self._begin_session(self.store.new_session_name(prefix="burst"))
        else:
            self.store.update_meta(self._session_name, {
                "burst": {"rate_hz": rate_hz, "trig_id": trig_id, "level": level,
                          "wall": time.time()}})

        # fast-fail if the reader thread dies instead of sitting out the timeout
        with self._recv_lock:
            self._recv_lock.wait_for(
                lambda: self._state != "BURST", timeout=timeout)
        self._raise_reader_error()
        return self._state == "STREAMING"

    def state(self):
        with self._lock:
            return self._state

    def reader_error(self):
        with self._lock:
            return self._reader_error

    def dispose(self):
        self.stop()
        if self.driver.connected:
            self.driver.disconnect()