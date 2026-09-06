"""MetaShunt V2 low-level serial driver.

Refactored from the original blocking implementation into:
  * pure module-level framing/control functions (easy to unit test, import
    anywhere), and
  * a thread-safe :class:`MetaShuntV2` driver whose read calls are designed to
    be driven by a single background capture thread.

Protocol summary
----------------
Measurement packets are framed on the wire as::

    0xAA <---- 8 payload bytes ----> <1 byte checksum>

where the checksum is the low byte of the sum of the 8 payload bytes.  The 8
payload bytes encode little-endian::

    uint32  tick        device time since reset, in QUARTERS of a microsecond
    float32 current_ma  instantaneous current in milliamps

So ``time_us = tick / 4.0``.  The tick is an unsigned 32-bit counter that wraps
every 2**32 ticks (~17.9 minutes at 4 ticks/us), so callers must unwrap.

Control commands share the same ``0xAA ... cksum`` framing::

    configure  : AA 02 05 <index> <float32>                -> sets a resistor
    read       : AA 03 01 <index>                          -> requests a param
    response   : AA 04 <float32> <index>                   -> server replies
    burst      : AA 01 04 <rate500> <trig> <lvl_hi> <lvl_lo>

Burst command fields
----------------------
``rate500`` is the requested rate in Hz divided by 500, rounded, max 255
(127.5 kHz max).  ``trig`` selects the trigger: 0 immediate, 1 rising edge,
2 falling edge, 3 stage index, 4 KEY2 button.  For rising/falling, ``lvl`` is a
16-bit little-endian value equal to ``round(level_uA / 5.0)`` (about 5 uA per
count).  A burst returns 37,500 measurement packets.  After a burst the device
automatically returns to continuous streaming mode.
"""

import array
import math
import struct
import time

import numpy as np
import serial
import serial.tools.list_ports

#: Index lookup for the on-board R-shunt config slots (not exposed over MCP).
config_index_dict = {
    "R19": 0,
    "R17": 1,
    "R15": 2,
    "R13": 3,
    "R11": 4,
    "R9": 5,
    "R2": 6,
    "R1": 7,
    "R_FET": 8,
}

#: Number of measurement packets returned by one burst read.
BURST_NUM_SAMPLES = 37500

#: USB FS vendor/product ids that identify a MetaShunt V2.
VID_METASHUNT = 1155
PID_METASHUNT = 22336

#: Default serial baud.  Over USB-FS this does not actually change the link
#: speed; kept for completeness / non-USB-FS adapters.
DEFAULT_BAUD = 1_000_000


class MEASUREMENT:
    """A single current sample.

    ``time`` holds the raw device tick (quarters of a microsecond); see
    :func:`unwrap_ticks` / :func:`ticks_to_us` for conversion.
    """

    __slots__ = ("time", "current_ma")

    def __init__(self, time, current_ma):
        self.time = time
        self.current_ma = current_ma


# ---------------------------------------------------------------------------
# Pure packet-layer helpers
# ---------------------------------------------------------------------------

def read_packet(ser, timeout):
    """Read exactly one framed measurement packet from *ser*.

    Returns the 8 payload bytes as a ``bytes`` object, or ``None`` on a
    checksum failure / timeout.  This is a non-blocking-in-spirit primitive:
    it loops on :meth:`serial.Serial.read` with a small timeout so it is safe
    to call from a dedicated reader thread.
    """
    step = 0
    count = 0
    chk = 0
    payload = bytearray()
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        data = ser.read(1)
        if not data:
            continue
        data = data[0]

        if step == 0 and data == 0xAA:
            step = 1
        elif step == 1:
            payload.append(data)
            count += 1
            chk = (chk + data) & 0xFF
            if count == 8:
                step = 2
        elif step == 2:
            if data == chk:
                return bytes(payload)
            # resync and keep looking within the remaining timeout
            step = 0
            count = 0
            chk = 0
            payload = bytearray()
    return None


def unpack_measurement(payload):
    """Decode an 8-byte measurement payload into ``(tick, current_ma)``."""
    info = struct.unpack("<If", bytes(payload))
    return info[0], info[1]


def read_config_response(ser, timeout):
    """Read a config response framed as ``AA 04 <index><float32> cksum``.

    Returns ``(index, value)`` or ``None`` on timeout/checksum failure.
    """
    step = 0
    count = 0
    chk = 0
    payload = bytearray()
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        data = ser.read(1)
        if not data:
            continue
        data = data[0]

        if step == 0 and data == 0xAA:
            step = 1
            chk = 0
        elif step == 1 and data == 0x04:
            step = 2
            chk = (chk + data) & 0xFF
            payload = bytearray()
            count = 0
        elif step == 1:
            continue  # not a config message, keep resyncing on 0xAA
        elif step == 2:
            payload.append(data)
            chk = (chk + data) & 0xFF
            count += 1
            if count == 5:
                step = 3
        elif step == 3:
            if data == chk:
                index, value = struct.unpack("<Bf", bytes(payload))
                return index, value
            step = 0
    return None


def _checksum(payload):
    """Low byte of the sum of payload bytes (bytes[1:], channel excludes the AA)."""
    chk = 0
    for b in payload[1:]:
        chk = (chk + b) & 0xFF
    return chk


def write_cmd(ser, payload):
    """Frame and write a control command as ``payload + cksum``."""
    buf = bytearray(payload)
    buf.append(_checksum(buf))
    ser.write(bytes(buf))


def build_burst_cmd(rate_hz, trig_id, level=0):
    """Assemble the burst-command payload bytes (without checksum).

    ``trig_id``: 0 immediate, 1 rising, 2 falling, 3 stage, 4 KEY2.
    ``level`` for rising/falling is in microamps (converted internally); for
    stage it is the raw stage index (0-65535).
    """
    if trig_id in (1, 2):
        level = int(round(level / 5.0))
    level = int(level) & 0xFFFF
    rate500 = max(1, min(255, int(round(rate_hz / 500.0))))
    return bytearray([
        0xAA, 0x01, 0x04, rate500, trig_id,
        (level >> 8) & 0xFF, level & 0xFF,
    ])


def ticks_to_us(ticks):
    """Convert device ticks to microseconds (ticks are quarters of a us)."""
    return np.asarray(ticks, dtype=np.float64) / 4.0


# ---------------------------------------------------------------------------
# Tick unwrapping
# ---------------------------------------------------------------------------

class TickUnwrapper:
    """Turn a raw u32 device tick stream into a monotonic us time base.

    The device tick wraps every 2**32.  This tracks the last seen tick and
    computes elapsed quarters-of-us from the most recent wrap, so ``elapsed_us``
    is monotonic across any number of wraparounds (it is not anchored to an
    absolute time).
    """

    MASK = np.uint64(0xFFFFFFFF)
    HALF = np.uint64(0x80000000)

    def __init__(self):
        self._last = None
        self._offset = np.uint64(0)

    def reset(self):
        self._last = None
        self._offset = np.uint64(0)

    def feed(self, tick):
        """Feed one raw tick, return the unwrapped elapsed time in us.

        A normal u32 wraparound produces a forward modulo delta (< half the
        range).  A device reset makes the raw tick jump *backward* by more
        than half the range, which we detect and treat as a re-anchor rather
        than a gigantic negative step.
        """
        tick = int(tick)
        if self._last is None:
            self._last = tick
            return 0.0
        delta = (tick - self._last) & 0xFFFFFFFF
        if delta >= 0x80000000:  # device reset: re-anchor
            self._last = tick
            self._offset = np.uint64(0)
            return 0.0
        self._last = tick
        self._offset = np.uint64(self._offset + delta)
        return float(self._offset) / 4.0


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

class MetaShuntV2:
    """Thread-safe driver for a single MetaShunt V2.

    Only one thread should ever call :meth:`read_one`; all other methods are
    guarded by a lock so configuration/connection can be managed from the MCP
    worker threads.
    """

    def __init__(self):
        self.ser = None
        self._lock = __import__("threading").RLock()

    # -- connection --------------------------------------------------------

    def find_port(self):
        """Locate the MetaShunt serial port by VID/PID; return '' if absent."""
        for comport in serial.tools.list_ports.comports():
            if comport.vid == VID_METASHUNT and comport.pid == PID_METASHUNT:
                return comport.device
        return ""

    def connect(self, port=None, baud=DEFAULT_BAUD):
        """Open the serial connection.  ``port`` auto-discovers if ``None``."""
        with self._lock:
            if self.ser is not None:
                return True
            if port is None:
                port = self.find_port()
            if not port:
                return False
            self.ser = serial.Serial(port, baudrate=baud, timeout=0.1)
            self.ser.reset_input_buffer()
            return True

    def disconnect(self):
        with self._lock:
            if self.ser is not None:
                try:
                    self.ser.close()
                finally:
                    self.ser = None

    @property
    def connected(self):
        with self._lock:
            return self.ser is not None

    # -- reading (single reader thread only) -------------------------------

    def flush_input(self):
        with self._lock:
            if self.ser is not None:
                self.ser.reset_input_buffer()

    def read_one(self, timeout=0.1):
        """Read one measurement; returns ``MEASUREMENT`` or ``None``.

        Meant to be called in a tight loop from the capture reader thread.
        """
        payload = read_packet(self.ser, timeout)
        if payload is None:
            return None
        tick, current_ma = unpack_measurement(payload)
        return MEASUREMENT(tick, current_ma)

    def send_burst(self, rate_hz, trig_id, level=0):
        """Send a burst command and flush stale bytes so the response parses cleanly."""
        with self._lock:
            self.ser.reset_input_buffer()
            write_cmd(self.ser, build_burst_cmd(rate_hz, trig_id, level))
            self.ser.reset_input_buffer()

    # -- configuration (kept for testing; not exposed over MCP) -------------

    def send_config(self, index, data):
        with self._lock:
            write_cmd(self.ser, struct.pack("<BBBBf", 0xAA, 2, 5, index, data))

    def request_config(self, index):
        with self._lock:
            self.ser.reset_input_buffer()
            write_cmd(self.ser, struct.pack("<BBBB", 0xAA, 3, 1, index))
            self.ser.reset_input_buffer()

    def get_config_param(self, key):
        with self._lock:
            time.sleep(0.1)
            self.request_config(config_index_dict[key])
            resp = read_config_response(self.ser, timeout=0.15)
            if resp is None:
                return None
            index, value = resp
            return value if index == config_index_dict[key] else None