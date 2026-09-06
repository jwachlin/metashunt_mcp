"""Runtime configuration for the MetaShunt MCP server.

All values can be overridden with environment variables so the server can be
launched from different hosts / log locations without code changes.
"""

import os
from pathlib import Path

from metashunt_mcp.metashunt_v2_lib import VID_METASHUNT, PID_METASHUNT

#: Serial baud.  Over USB-FS this value is cosmetic (the link runs at the
#: device's native rate regardless), but is honoured for other adapters.
BAUD = int(os.environ.get("METASHUNT_BAUD", "1000000"))

#: VID/PID used to auto-discover the MetaShunt on the serial bus.
METASHUNT_VID = int(os.environ.get("METASHUNT_VID", VID_METASHUNT))
METASHUNT_PID = int(os.environ.get("METASHUNT_PID", PID_METASHUNT))

#: Where named session logs are persisted between server runs.
DEFAULT_LOG_DIR = Path.home() / ".metashunt" / "logs"
LOG_DIR = Path(os.environ.get("METASHUNT_LOG_DIR", str(DEFAULT_LOG_DIR))).expanduser()

#: Maximum number of samples buffered in-memory for the live stream / burst
#: ring buffer.  At ~6.2 kHz this is ~27 seconds of continuous data.
RING_BUFFER_SIZE = int(os.environ.get("METASHUNT_RING_SIZE", "262144"))

#: Consider the device disconnected if no sample arrives in this many seconds.
READER_IDLE_TIMEOUT = float(os.environ.get("METASHUNT_IDLE_TIMEOUT", "2.0"))

#: Time in seconds a single non-blocking serial read waits before giving up.
SERIAL_READ_TIMEOUT = float(os.environ.get("METASHUNT_SERIAL_READ_TIMEOUT", "0.1"))

#: Decimation defaults.  These mirror the tool defaults but are centralized.
DEFAULT_THRESHOLD_PCT = float(os.environ.get("METASHUNT_DEC_THRESHOLD_PCT", "1.0"))
DEFAULT_ABS_MIN_THRESHOLD_MA = float(os.environ.get("METASHUNT_DEC_ABS_MIN_MA", "0.000030"))
DEFAULT_MAX_SAMPLES = int(os.environ.get("METASHUNT_DEC_MAX_SAMPLES", "1000"))
DEFAULT_MAX_POINTS = int(os.environ.get("METASHUNT_DEC_MAX_POINTS", "1000"))


def ensure_log_dir():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR