"""Shared helpers: a fake MetaShunt serial device for tests."""

import struct
import threading

import numpy as np

from metashunt_mcp.metashunt_v2_lib import _checksum


def frame_packet(tick: int, current_ma: float) -> bytes:
    """Serialize one measurement packet exactly as the device does."""
    payload = bytearray([0xAA]) + bytearray(struct.pack("<If", tick, current_ma))
    payload.append(_checksum(payload))
    return bytes(payload)


class FakeSerial:
    """Emulates the MetaShunt serial byte stream.

    ``reset_input_buffer()`` really clears queued bytes (as a USB-serial chip
    would) and a burst command's response is delivered on a short async delay,
    mirroring the device's buffered-drain behaviour.
    """

    def __init__(self):
        self.buffer = bytearray()
        self.writes = []
        self._timers = []

    def feed(self, packets):
        with self._buf_lock():
            for p in packets:
                self.buffer.extend(p)

    def _buf_lock(self):
        return _BufferLock(self)

    def reset_input_buffer(self):
        with self._buf_lock():
            self.buffer.clear()

    def close(self):
        for t in self._timers:
            t.cancel()

    def read(self, n):
        with self._buf_lock():
            if not self.buffer:
                return b""
            out = bytes(self.buffer[:n])
            del self.buffer[:n]
            return out

    def write(self, data):
        self.writes.append(bytes(data))
        # Burst command (0xAA 0x01 ...): the device responds asynchronously with
        # the 37,500 burst samples once it has drained its own buffer.
        # byte[3] is rate500 = requested Hz / 500, so the fake measures samples
        # at the actual device rate (rtc = 4e6 / (rate500*500) ticks apart).
        if len(data) >= 4 and data[0] == 0xAA and data[1] == 0x01:
            rate500 = data[3]
            t = threading.Timer(0.05, self._deliver_burst, args=(rate500,))
            t.daemon = True
            self._timers.append(t)
            t.start()
        return len(data)

    def _deliver_burst(self, rate500):
        dt_ticks = max(1, int(round(4e6 / (rate500 * 500))))
        self.feed([frame_packet(i * dt_ticks, 0.8 + 0.001 * i)
                   for i in range(1, 37501)])


class _BufferLock:
    """Tiny helper so FakeSerial uses a shared lock around the byte buffer."""

    def __init__(self, fake):
        self._lock = fake.__dict__.setdefault("_mutex", threading.Lock())

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *exc):
        self._lock.release()


def make_stream(tick0=0, n=20000, fs=6200.0, spike_frac=0.45, spike_ms=5.0):
    """Synthesize n (tick, mA) samples: 100 uA flat with a spike_ms 4 mA wakeup.

    The spike is placed at ``spike_frac`` of the way through the stream so it
    is always inside the sample window.
    """
    step = max(1, int(round(4e6 / fs)))  # quarter-us per sample
    times = tick0 + np.arange(n) * step
    cur = np.full(n, 0.1)
    w0 = int(n * spike_frac)
    w1 = min(n, w0 + int(spike_ms * fs / 1000.0))
    cur[w0:w1] = 4.0
    return [(int(ti), float(m)) for ti, m in zip(times, cur)]


def packets_for(samples):
    return [frame_packet(t, c) for t, c in samples]