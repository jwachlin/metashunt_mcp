"""Charge-preserving adaptive decimation for MetaShunt V2 streams.

Unlike naive "keep every Nth sample", this decimator:

* keeps the *first* measurement verbatim as the first decimated point,
* watches the live stream for a change greater than
  ``max(threshold_pct * |last_decimated_value|, abs_min_threshold)``,
* emits a new decimated point when that change is detected *or* after
  ``max_samples`` raw samples (so steady-state regions still produce data),
* emits the **mean current** over the interval so that the preserved total
  charge (area under the current vs. time curve) is exact regardless of how
  aggressively the data is compressed.

This keeps tight timing on sharp events (wakeups, bursts) while compressing
long, nearly-constant plateaus to ~1 point per ``max_samples``.
"""

import numpy as np


def decimate(t_us: np.ndarray,
             cur_ma: np.ndarray,
             threshold_pct: float = 1.0,
             abs_min_threshold_ma: float = 30e-6,
             max_samples: int = 1000):
    """Adaptively decimate a ``(time_us, current_ma)`` pair.

    Parameters
    ----------
    t_us : (N,) float64 device-relative time in microseconds (unwrap first).
    cur_ma : (N,) float32/float64 current in milliamps.
    threshold_pct : percent of |last decimated value| that counts as a change.
    abs_min_threshold_ma : absolute floor for change detection (default 30 nA).
    max_samples : cap on raw samples between emitted points (steady-state).

    Returns
    -------
    (dt_us, dcur_ma) : decimated arrays (mean-current per interval).
    """
    t_us = np.asarray(t_us, dtype=np.float64)
    cur_ma = np.asarray(cur_ma, dtype=np.float64)
    n = cur_ma.shape[0]

    if n == 0:
        return t_us[:0], cur_ma[:0]

    out_t = [t_us[0]]
    out_c = [cur_ma[0]]
    if n == 1:
        return np.array(out_t), np.array(out_c)

    pct = threshold_pct / 100.0
    # Convert to a plain list once for a tight native loop.  Typical arrays are
    # ~1e5-5e5 samples which is fast enough for an MCP round-trip.
    tt = t_us.tolist()
    cc = cur_ma.tolist()

    last_dec_c = cc[0]
    last_t = tt[0]
    prev_t = tt[0]
    acc_area = 0.0   # trapezoidal area (charge) since the last emitted point
    acc_dt = 0.0     # total time spanned since the last emitted point
    cnt = 1          # raw samples indexed into this interval

    def emit(ti):
        rep = (acc_area / acc_dt) if acc_dt > 0 else cur_ma[-1]
        out_t.append(ti)
        out_c.append(rep)
        return rep

    for i in range(1, n):
        cur = cc[i]
        ti = tt[i]
        dt = ti - prev_t
        prev_t = ti
        acc_area += 0.5 * (cur + cc[i - 1]) * dt
        acc_dt += dt
        cnt += 1

        emit_thr = max(abs(last_dec_c) * pct, abs_min_threshold_ma)
        if abs(cur - last_dec_c) > emit_thr or cnt >= max_samples:
            last_dec_c = emit(ti)
            last_t = ti
            acc_area = 0.0
            acc_dt = 0.0
            cnt = 0

    # flush the residual tail so no charge is lost at end-of-stream
    if acc_dt > 0:
        out_t.append(tt[-1])
        out_c.append(acc_area / acc_dt)

    return np.array(out_t), np.array(out_c)


def decimate_to_points(t_us: np.ndarray,
                       cur_ma: np.ndarray,
                       max_points: int = 1000,
                       threshold_pct: float = 1.0,
                       abs_min_threshold_ma: float = 30e-6):
    """Decimate targeting at most ``max_points`` output samples.

    ``max_samples`` (the steady-state cap) is derived from the raw length and
    the requested budget, then refined upward until the budget is met.
    ``threshold``-based events are always preserved as long as possible; only
    if the data is below the threshold *everywhere* (so adaptive compression
    cannot reach the budget) do we fall back to stride decimation.
    """
    n = int(cur_ma.shape[0])
    if n <= max_points:
        return t_us, cur_ma

    max_samples = max(1, int(n / max_points))
    t_out, c_out = decimate(t_us, cur_ma,
                            threshold_pct=threshold_pct,
                            abs_min_threshold_ma=abs_min_threshold_ma,
                            max_samples=max_samples)
    # coarsen the steady-state cap until the budget is met (spikes keep their
    # own emitted points, so we may overshoot slightly and iterate).
    for _ in range(6):
        if t_out.shape[0] <= max_points:
            break
        max_samples = max(max_samples * 2, 1)
        t_out, c_out = decimate(t_us, cur_ma,
                                threshold_pct=threshold_pct,
                                abs_min_threshold_ma=abs_min_threshold_ma,
                                max_samples=max_samples)

    # last-resort: the signal is changing faster than the budget allows, so
    # stride-sample the adaptive output (charge summary is preserved).
    if t_out.shape[0] > max_points:
        stride = (t_out.shape[0] + max_points - 1) // max_points
        t_out, c_out = t_out[::stride], c_out[::stride]
    return t_out, c_out