# MetaShunt V2 MCP server

A local MCP server that turns a MetaShunt V2 into a firmware-development
current-measurement tool.  You (running inside OpenCode / OpenRouter) get a
clean tool interface for continuous streaming at ~6.2 kHz, high-rate burst
reads up to 127.5 kHz (37,500 samples), threshold/edge detection on the live
stream, and named, persistent capture sessions so you can keep data across
firmware iterations and server restarts.

## Architecture

```
OpenCode (you, model = OpenRouter)
   │   stdio (MCP)
   ▼
metashunt-mcp                     (this package)
   │   serial — USB FS, VID 1155 PID 22336 (auto-discovered)
   ▼
MetaShunt V2  ──▶  measures from ~10s of nA to ~2 A
```

- **Background capture thread.**  A single thread owns the serial port from
  the moment the server starts and reads measurement packets continuously into
  an in-memory ring buffer.  Data is therefore always flowing and immediately
  queryable regardless of which tool you call.
- **Monotonic device-time base.**  Every sample carries the device's raw u32
  tick (4 ticks/µs → us = ticks / 4).  The 32-bit tick wraps every ~17.9
  minutes and the device may reset; both cases are unwrapped/re-anchored so
  burst data stays time-aligned with the continuous stream even though USB-FS
  deliverst the device's 32,000-sample buffer to the host with wall-clock
  delay.
- **Charge-preserving adaptive decimation** (see below) keeps the MCP
  responses small without ever "missing" a current spike.
- **No shunt control.**  The MetaShunt automatically switches its R-shunt
  resistors to keep burden voltage low while measuring accurately from tens of
  nA to ~2 A.  The server never touches them, and does not expose them.

## Install

Requires `uv` (see https://docs.astral.sh/uv/).

```bash
cd metashunt_mcp
uv sync
uv run metashunt-mcp          # runs the stdio server (see below)
```

## Self-test (before wiring up an LLM)

A single command walks the whole pipeline — device discovery, connection,
continuous streaming, storage, and (optionally) a burst read — and returns a
non-zero exit code if any stage fails:

```bash
# against a real, plugged-in MetaShunt
uv run python scripts/selftest.py

# offline sanity check of the plumbing (no hardware)
uv run python scripts/selftest.py --simulate

# include a device burst read
uv run python scripts/selftest.py --burst           # or --simulate --burst
```

Options: `-t SECONDS` (stream duration, default 3), `--rate HZ` (burst rate),
`--log-dir DIR` (where the test session lands, default `/tmp/msx_selftest`),
`--simulate` (offline).  Successful output ends with `SELF-TEST PASSED` and
reports the live mean current and on-disk sample count so you can eyeball
plausibility before any model connects.

## Register with OpenCode

Merge `clients/opencode.json`'s `mcp.metashunt` block into your opencode
configuration (e.g. a project-root `opencode.json`) so OpenCode launches the
server locally over stdio:

```json
{
  "mcp": {
    "metashunt": {
      "type": "stdio",
      "command": "metashunt-mcp",
      "args": []
    }
  }
}
```

Make sure `metashunt-mcp` is on `PATH` for the environment OpenCode uses
(`uv run metashunt-mcp` works if you put the repo on PATH, or set `command` to
the absolute path of `metashunt-mcp`).

### Read-only server (limited model access)

If you want to give a model measurement access without letting it change
anything, register the **read-only** server instead of the full one.  It
exposes only observe-only tools (`metashunt_status`, `metashunt_stream_now`,
`metashunt_list_sessions`, `metashunt_load_session`, `metashunt_slice_log`,
`metashunt_measurement_stats`, `metashunt_list_triggers`) and drops every
mutating tool (`start_capture`, `stop_capture`, `burst`, `set_trigger`,
`remove_trigger`, `delete_session`).  Use `clients/opencode.readonly.json`:

```json
{
  "mcp": {
    "metashunt": {
      "type": "local",
      "command": ["metashunt-mcp-readonly"],
      "enabled": true
    }
  }
}
```

You can layer OpenCode permission rules on top (`permission` in
`opencode.json`) as a second line of defense, e.g. `ask`/`deny` on the tool
ids `mcp__metashunt__*`.  The read-only server uses the same underlying
capture engine; it simply never advertises the mutating tools.

## The measurement protocol (what the server does)

Each on-wire measurement packet is framed as:

```
0xAA | U32 tick  (4/µs) | F32 current_mA | 8-bit checksum
```

Commands use the same framing.  The **burst** command requests a 37,500-sample
read at up to 127.5 kHz and accepts a trigger (immediate, current rising,
current falling, stage index, or the KEY2 button).  After a burst the device
**automatically returns to streaming**; the server just switches back to
recording the continuous stream.  **Streaming needs no stop command** — the
server simply stops listening when you call `metashunt_stop_capture`, and the
device keeps measuring on its own.

The device logic this server relies on lives in
`src/metashunt_mcp/metashunt_v2_lib.py` (framing, burst-command building, tick
unwrapping, and the thread-safe `MetaShuntV2` driver).

## Decimation (important to understand)

Every data-returning tool runs the live stream / stored log through an
**adaptive, charge-preserving** decimator rather than "keep every Nth sample", so
that sharp events (a wake-up current spike, a burst edge) are preserved at full
timing resolution while long steady plateaus compress hard.

Parameters (accepted by every data-returning tool):

- `max_points` — soft cap on the number of returned points (default 1000).
- `threshold_pct` — percent of the *last decimated value* that counts as a
  change worth emitting (default 1.0).
- `abs_min_threshold_ma` — absolute floor for that change, so quiet signals
  still react (default 30 nA = 0.00003 mA).

Algorithm:

1. The **first** measurement becomes the first decimated value.
2. The emit threshold is `max(threshold_pct × |last_decimated_value|,
   abs_min_threshold_ma)`.
3. Raw samples accumulate their trapezoidal integral (Δt × mean(current)) —
   i.e. charge — plus a running time span.
4. A new decimated point is emitted when either:
   - current strays from the last decimated value by more than the emit
     threshold (a genuine change / spike / wake-up), **or**
   - `max_samples` raw samples have elapsed since the last point (steady-state
     guarantee that flat data still yields a point every `max_samples` samples).
5. The emitted value is the **time-mean of current** over that interval, so the
   total area under the current curve (charge consumed) is preserved exactly no
   matter how aggressively the data is compressed.  The interval is stamped at
   its closing wall-clock-of-device-time.
6. A residual tail at end-of-data is flushed so no charge is lost.

`metashunt_*` tools that return points also return a `count` and summary stats
so you can sanity-check how much was compressed.  A noise-heavy signal that
cannot reach the `max_points` budget falls back to stride sampling of the
adaptive output.

Example: a 10 s / 6.2 kHz stream that is flat except for one 5 ms 4 mA wake-up
compresses to a handful of points (≈1000× reduction) while keeping the 4 mA
peak and ~0.02% charge error.

## Tools

| Tool | Purpose |
|---|---|
| `metashunt_status` | connection state, capture state, ring fill, last sample, reader health |
| `metashunt_start_capture(session?, label?, meta?)` | begin continuous streaming into a named session (default: timestamped auto-name) |
| `metashunt_stop_capture` | stop recording/streaming; device keeps measuring |
| `metashunt_stream_now(max_points?, threshold_pct?, abs_min_threshold_ma?)` | decimated tail of the live ring buffer |
| `metashunt_burst(rate_hz?, trigger?, level?, max_points?...)` | device burst read (37,500 samples, up to 127.5 kHz) |
| `metashunt_set_trigger(name, above_ma?, below_ma?)` | host-side threshold/edge detector on the live stream |
| `metashunt_list_triggers` | list current trigger rules |
| `metashunt_remove_trigger(name)` | remove a trigger rule |
| `metashunt_list_sessions` | list persisted sessions (name, sample count, time span) |
| `metashunt_load_session(session, decimate args...)` | load a session from disk, decimated |
| `metashunt_slice_log(session, start_t_us?, end_t_us?, decimate args...)` | load a time window of a session |
| `metashunt_measurement_stats(session, window?)` | count/mean/std/min/max over a window (no bulk transfer) |
| `metashunt_delete_session(session)` | delete a session (parts + meta) |

Time arguments (`start_t_us`, `end_t_us`) are in device-time microseconds
relative to device reset/metashunt power-on (the unwrapped tick base), matching
the `t_us` values returned by all tools.

### Burst trigger options

`metashunt_burst(rate_hz, trigger, level)` where `trigger`:

- `immediate` — capture immediately
- `rising` — begin once current rises above `level` uA
- `falling` — begin once current falls below `level` uA
- `stage` — begin at the given stage index (`level`)
- `key2` — begin on the device KEY2 button

`rate_hz` is rounded to the device's 500 Hz steps (max 127.5 kHz).  When
triggered by current, `level` is the current in µA (the device encodes at
5 µA/LSB).

## Example workflow (firmware testing)

1. `metashunt_status` — confirm it sees the device and is `IDLE`.
2. `metashunt_set_trigger("wakeup", above_ma=1.0)` — arm host-side detection of wake-ups.
3. `metashunt_start_capture(label="fw_test_run_42")` — start streaming to a session.
4. Exercise the firmware under test.
5. `metashunt_stream_now(max_points=500)` — glance at the live tail; a wake-up spike shows up as a point near mA-level even though the nominal current is nA/µA.
6. For a high-rate view, `metashunt_burst(rate_hz=100000, trigger="rising", level=500)` right before the event — the device captures 37,500 samples and the server returns them decimated.
7. `metashunt_stop_capture` — finalize the session (device keeps sampling on its own).
8. `metashunt_list_sessions` → `metashunt_load_session(...)` later to compare against the next firmware iteration.

## Storage format (what survives on disk)

Sessions live in `~/.metashunt/logs/` (override with `METASHUNT_LOG_DIR`) as:

```
<session>.json
<session>.p0001.npz
<session>.p0002.npz
...        (one part per disk flush, appended in order)
```

- Each `.p####.npz` holds the arrays `t_us` (float64, µs), `current_ma`
  (float32), and `kind` (`'stream'` or `'burst'`) so continuous and burst data
  stay separable.  Parts are **append-only and atomic**: a flush writes a new
  part file and never rewrites old ones, which keeps appending cheap and
  immune to the read-modify-write races that plagued single-file logging.
- `<session>.json` records session metadata: label, started/ended wall times,
  last flush time, and burst/event annotations.
- `list_sessions` / `load_session` / `slice_log` / `measurement_stats` read
  these files, so sessions persist across server restarts.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `METASHUNT_BAUD` | 1000000 | baud for the serial link (cosmetic over USB-FS, honored for other adapters) |
| `METASHUNT_LOG_DIR` | `~/.metashunt/logs` | where sessions are persisted |
| `METASHUNT_RING_SIZE` | 262144 | in-memory raw ring capacity (samples) |
| `METASHUNT_IDLE_TIMEOUT` | 2.0 | reserved (idle detection) |
| `METASHUNT_SERIAL_READ_TIMEOUT` | 0.1 | per-read serial timeout (s) |
| `METASHUNT_DEC_THRESHOLD_PCT` | 1.0 | default decimation threshold percent |
| `METASHUNT_DEC_ABS_MIN_MA` | 0.00003 | default abs-min decimation threshold (mA) |
| `METASHUNT_DEC_MAX_POINTS` | 1000 | default `max_points` |
| `METASHUNT_SELFTEST_SIMULATE` | (unset) | set to `1` to force `scripts/selftest.py` into offline mode |

## Notes and limitations

- The MCP surface intentionally has **no R-shunt / configuration tools**: the
  device manages shunt switching autonomously to keep burden voltage low while
  measuring from ~10s of nA to ~2 A.
- The original blocking config read/write device functions still exist in
  `metashunt_v2_lib.py` for manual hardware testing, but are not exposed as
  MCP tools.
- Transport is **stdio only**; the serial I/O stays on the host machine where
  the MetaShunt is plugged in (OpenCode launches the server locally).  No
  cloud / OpenRouter side ever touches the USB device.
- Device time is relative (unwrapped ticks since power-on/reset), not absolute
  wall clock; use the `t_us` values in tool responses for any differencing.