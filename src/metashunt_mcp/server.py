"""MCP (stdio) server for MetaShunt V2.

Runs as a long-lived process launched by OpenCode.  A background capture
thread continuously reads the MetaShunt; the tools here expose snapshots,
decimated slices, burst reads, trigger rules and session logs over stdio.

Auto-shunt switching is entirely onboard the device and is intentionally not
exposed.
"""

import threading
from typing import Optional

import numpy as np
from mcp.server.fastmcp import FastMCP

from metashunt_mcp import config, decimate
from metashunt_mcp.capture import CaptureEngine, KIND_BURST, KIND_STREAM

mcp = FastMCP("metashunt")

#: Single engine shared by all tools.
_engine = None
_engine_lock = threading.Lock()

_TRIG_IDS = {"immediate": 0, "rising": 1, "falling": 2, "stage": 3, "key2": 4}

#: Subset of tools that only OBSERVE (no state changes, no device commands,
#: no session mutation).  The read-only server variant exposes exactly these.
READONLY_TOOLS = (
    "metashunt_status",
    "metashunt_stream_now",
    "metashunt_list_sessions",
    "metashunt_load_session",
    "metashunt_slice_log",
    "metashunt_measurement_stats",
    "metashunt_list_triggers",
)


def _get_engine() -> CaptureEngine:
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = CaptureEngine()
                _engine.start()
    return _engine


def _summarize(t_us, cur_ma):
    if t_us.shape[0] == 0:
        return {"count": 0}
    return {
        "count": int(t_us.shape[0]),
        "t_start_us": float(t_us[0]),
        "t_end_us": float(t_us[-1]),
        "mean_ma": float(cur_ma.mean()),
        "min_ma": float(cur_ma.min()),
        "max_ma": float(cur_ma.max()),
        "std_ma": float(cur_ma.std()),
    }


# --------------------------------------------------------------------------- #
# status / lifecycle
# --------------------------------------------------------------------------- #

@mcp.tool()
def metashunt_status() -> dict:
    """Return connection and capture status."""
    eng = _get_engine()
    n, mean, std = eng.ring_stats()
    last = eng.ring_last()
    return {
        "connected": eng.driver.connected,
        "state": eng.state(),
        "reader_ok": eng.reader_error() is None,
        "reader_error": str(eng.reader_error()) if eng.reader_error() else None,
        "sessions_dir": str(eng.store.log_dir),
        "ring_size": int(eng._count),
        "ring_capacity": int(eng._cap),
        "ring_mean_ma": mean,
        "ring_std_ma": std,
        "last_sample": {"t_us": last[0], "cur_ma": last[1]} if last else None,
        "trigger_rules": eng.triggers.list_rules(),
    }


@mcp.tool()
def metashunt_start_capture(session: Optional[str] = None,
                            label: Optional[str] = None,
                            meta: Optional[dict] = None) -> str:
    """Begin continuous (streaming) capture into a named session.

    Returns the session name used.  Sampling continues in the background until
    :func:`metashunt_stop_capture` is called.
    """
    eng = _get_engine()
    if session is None:
        session = eng.store.new_session_name(label=label)
    aug = dict(meta or {})
    if label:
        aug["label"] = label
    return eng.start_stream(session, meta=aug)


@mcp.tool()
def metashunt_stop_capture() -> dict:
    """Stop recording.  The device keeps measuring; the server just stops
    listening.  The current session is finalized on disk."""
    eng = _get_engine()
    eng.stop_stream()
    return {"state": eng.state()}


# --------------------------------------------------------------------------- #
# live data
# --------------------------------------------------------------------------- #

@mcp.tool()
def metashunt_stream_now(max_points: int = config.DEFAULT_MAX_POINTS,
                         threshold_pct: float = config.DEFAULT_THRESHOLD_PCT,
                         abs_min_threshold_ma: float = config.DEFAULT_ABS_MIN_THRESHOLD_MA) -> dict:
    """Return the most recent samples from the live ring, decimated.

    ``max_points`` caps the output; the adaptive decimator preserves charge and
    keeps spike timing.  Returns decimated (t_us, cur_ma) plus a summary.
    """
    eng = _get_engine()
    t, c, k = eng.ring_tail(eng._count)
    kind_mask = k == KIND_STREAM
    t, c = t[kind_mask], c[kind_mask]
    if max_points and t.shape[0] > max_points:
        t, c = decimate.decimate_to_points(t, c, max_points=max_points,
                                           threshold_pct=threshold_pct,
                                           abs_min_threshold_ma=abs_min_threshold_ma)
    return {
        "t_us": t.tolist(),
        "cur_ma": c.tolist(),
        **(_summarize(t, c)),
    }


@mcp.tool()
def metashunt_burst(rate_hz: int = 1000,
                    trigger: str = "immediate",
                    level: float = 0.0,
                    max_points: int = config.DEFAULT_MAX_POINTS,
                    threshold_pct: float = config.DEFAULT_THRESHOLD_PCT,
                    abs_min_threshold_ma: float = config.DEFAULT_ABS_MIN_THRESHOLD_MA) -> dict:
    """Perform a high-rate device burst read.

    ``trigger``: immediate | rising | falling | stage | key2.
    ``level``: current in uA for rising/falling, or the stage index for stage.
    Returns up to 37,500 samples at up to 127.5 kHz, decimated for return.
    """
    eng = _get_engine()
    trig_id = _TRIG_IDS.get(trigger.lower() if trigger else "")
    if trig_id is None:
        raise ValueError("trigger must be one of: " + ", ".join(_TRIG_IDS))
    boundary = eng.ring_head()
    ok = eng.request_burst(rate_hz, trig_id, level=level)
    # pull out only the samples that arrived for this burst
    t, c, k = eng.ring_after(boundary)
    kind_mask = k == KIND_BURST
    t, c = t[kind_mask], c[kind_mask]
    if max_points and t.shape[0] > max_points:
        t, c = decimate.decimate_to_points(t, c, max_points=max_points,
                                           threshold_pct=threshold_pct,
                                           abs_min_threshold_ma=abs_min_threshold_ma)
    return {
        "completed": bool(ok),
        "burst_samples_raw": int((k == KIND_BURST).sum()),
        "t_us": t.tolist(),
        "cur_ma": c.tolist(),
        **(_summarize(t, c)),
    }


# --------------------------------------------------------------------------- #
# triggers (host-side)
# --------------------------------------------------------------------------- #

@mcp.tool()
def metashunt_set_trigger(name: str,
                          above_ma: Optional[float] = None,
                          below_ma: Optional[float] = None) -> dict:
    """Add/replace a host-side trigger watching the live stream for a crossing
    above ``above_ma`` and/or below ``below_ma`` (mA)."""
    eng = _get_engine()
    eng.triggers.add_rule(name, above_ma=above_ma, below_ma=below_ma)
    return {"trigger": name, "above_ma": above_ma, "below_ma": below_ma}


@mcp.tool()
def metashunt_list_triggers() -> dict:
    eng = _get_engine()
    return {"triggers": eng.triggers.list_rules()}


@mcp.tool()
def metashunt_remove_trigger(name: str) -> dict:
    eng = _get_engine()
    eng.triggers.remove_rule(name)
    return {"removed": name}


# --------------------------------------------------------------------------- #
# session logs
# --------------------------------------------------------------------------- #

@mcp.tool()
def metashunt_list_sessions() -> dict:
    """List persisted capture sessions (from .npz/.json logs)."""
    eng = _get_engine()
    return {"sessions": eng.store.list_sessions()}


@mcp.tool()
def metashunt_load_session(session: str,
                           max_points: int = config.DEFAULT_MAX_POINTS,
                           threshold_pct: float = config.DEFAULT_THRESHOLD_PCT,
                           abs_min_threshold_ma: float = config.DEFAULT_ABS_MIN_THRESHOLD_MA) -> dict:
    """Load a session from disk, decimate, and return its data + summary."""
    eng = _get_engine()
    t, c, kind = eng.store.load(session)
    t = t.astype(np.float64)
    c = c.astype(float)
    if max_points and t.shape[0] > max_points:
        t, c = decimate.decimate_to_points(t, c, max_points=max_points,
                                           threshold_pct=threshold_pct,
                                           abs_min_threshold_ma=abs_min_threshold_ma)
    return {
        "session": session,
        "t_us": t.tolist(),
        "cur_ma": c.tolist(),
        **(_summarize(t, c)),
    }


@mcp.tool()
def metashunt_slice_log(session: str,
                        start_t_us: Optional[float] = None,
                        end_t_us: Optional[float] = None,
                        max_points: int = config.DEFAULT_MAX_POINTS,
                        threshold_pct: float = config.DEFAULT_THRESHOLD_PCT,
                        abs_min_threshold_ma: float = config.DEFAULT_ABS_MIN_THRESHOLD_MA) -> dict:
    """Load a time window ``[start_t_us, end_t_us]`` of a session on disk."""
    eng = _get_engine()
    t, c, _ = eng.store.load(session)
    mask = np.ones(t.shape, dtype=bool)
    if start_t_us is not None:
        mask &= t >= start_t_us
    if end_t_us is not None:
        mask &= t <= end_t_us
    t, c = t[mask], c[mask]
    if max_points and t.shape[0] > max_points:
        t, c = decimate.decimate_to_points(t, c, max_points=max_points,
                                           threshold_pct=threshold_pct,
                                           abs_min_threshold_ma=abs_min_threshold_ma)
    return {
        "session": session,
        "t_us": t.tolist(),
        "cur_ma": c.tolist(),
        **(_summarize(t, c)),
    }


@mcp.tool()
def metashunt_measurement_stats(session: str,
                                start_t_us: Optional[float] = None,
                                end_t_us: Optional[float] = None) -> dict:
    """Compute aggregate statistics (count/mean/std/min/max) over a session
    window without transferring the full dataset."""
    eng = _get_engine()
    t, c, _ = eng.store.load(session)
    mask = np.ones(t.shape, dtype=bool)
    if start_t_us is not None:
        mask &= t >= start_t_us
    if end_t_us is not None:
        mask &= t <= end_t_us
    t, c = t[mask], c[mask]
    return _summarize(t, c)


@mcp.tool()
def metashunt_delete_session(session: str) -> dict:
    eng = _get_engine()
    removed = eng.store.delete(session)
    return {"session": session, "deleted": bool(removed)}


def main():
    mcp.run()


def main_readonly():
    """Run a strict read-only server exposing only observe-only tools.

    Use this when you want the model to be able to read measurements but never
    start/stop captures, send device burst commands, or delete sessions.  The
    read-only variant reuses the exact same handlers as the full server; it
    simply refuses to advertise the mutating tools.
    """
    global mcp
    ro = FastMCP("metashunt-readonly")
    for name in READONLY_TOOLS:
        ro.tool(name=name)(globals()[name])
    mcp = ro
    ro.run()


if __name__ == "__main__":
    main()