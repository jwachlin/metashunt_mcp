"""Persistent named-session storage for MetaShunt captures.

Each session is a timestamped, optionally user-named dataset.  Samples are
stored as a sequence of **append-only part files**::

    <log_dir>/<session>.json        metadata (started/ended, bursts, events)
    <log_dir>/<session>.p0001.npz   t_us, current_ma, kind
    <log_dir>/<session>.p0002.npz   ... (one part per flush)
    ...

Append-only parts mean a flush never has to read back and rewrite earlier data
(so appends stay cheap and atomic — a file is only ever written once).  Load
concatenates parts in order.  Sessions survive server restarts, letting a
firmware-development workflow keep before/after capture data for comparison.
"""

import json
import re
import threading
import time
from pathlib import Path

import numpy as np

from metashunt_mcp import config

#: Allowed characters for a session name so it is safe to use as a filename.
_NAME_RE = re.compile(r"^[a-zA-Z0-9_.\-]+$")
_PART_RE = re.compile(r"^(?P<name>.*)\.p(?P<num>\d{4})\.npz$")


class SessionStore:
    def __init__(self, log_dir=None):
        self.log_dir = Path(log_dir) if log_dir else config.LOG_DIR
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    # -- naming -----------------------------------------------------------

    @staticmethod
    def _sanitize(label):
        label = (label or "").strip()
        if label and _NAME_RE.match(label):
            return label
        return None

    def new_session_name(self, label=None, prefix="cap"):
        base = time.strftime("%Y%m%d_%H%M%S")
        safe = self._sanitize(label)
        stem = f"{prefix}_{base}" if not safe else f"{prefix}_{base}_{safe}"
        candidate, k = stem, 1
        while self.parts(candidate):
            candidate = f"{stem}_{k}"
            k += 1
        return candidate

    def _meta(self, name):
        return self.log_dir / f"{name}.json"

    def parts(self, name):
        """Return the part files for *name*, sorted by sequence number."""
        with self._lock:
            parts = []
            for p in self.log_dir.glob(f"{name}.p*.npz"):
                m = _PART_RE.match(p.name)
                if m and m.group("name") == name:
                    parts.append((int(m.group("num")), p))
            parts.sort()
            return [p for _, p in parts]

    # -- writing ----------------------------------------------------------

    def append(self, name, t_us, current_ma, kind="stream"):
        """Atomically append samples to a new part file of *name*.

        Kind tags the source stream so burst and continuous data stay
        separable: 'stream' or 'burst'.
        """
        t_us = np.asarray(t_us, dtype=np.float64)
        current_ma = np.asarray(current_ma, dtype=np.float32)
        kinds = np.asarray(kind, dtype="S8")
        if kinds.ndim == 0:
            kinds = np.full(current_ma.shape[0], kinds.item(), dtype="S8")
        elif kinds.shape[0] != current_ma.shape[0]:
            kinds = np.full(current_ma.shape[0], kind, dtype="S8")
        kinds = kinds.astype("S8")

        with self._lock:
            existing = self.parts(name)
            num = (int(existing[-1].name.split(".p")[1].split(".npz")[0])
                   if existing else 0) + 1
            p = self.log_dir / f"{name}.p{num:04d}.npz"
            np.savez_compressed(p, t_us=t_us, current_ma=current_ma, kind=kinds)
        self.update_meta(name, {"last_wall": time.time()})

    def save_meta(self, name, meta):
        with self._lock:
            self._meta(name).write_text(json.dumps(meta, indent=2))

    def update_meta(self, name, patch):
        with self._lock:
            p = self._meta(name)
            data = {}
            if p.exists():
                try:
                    data = json.loads(p.read_text())
                except (json.JSONDecodeError, OSError):
                    data = {}
            data.update(patch)
            data["name"] = data.get("name", name)
            p.write_text(json.dumps(data, indent=2))

    # -- listing / loading ---------------------------------------------------

    def list_sessions(self):
        with self._lock:
            names = sorted({_PART_RE.match(p.name).group("name")
                            for p in self.log_dir.glob("*.p*.npz")
                            if _PART_RE.match(p.name)})
            sessions = []
            for name in names:
                meta = {}
                mp = self._meta(name)
                if mp.exists():
                    try:
                        meta = json.loads(mp.read_text())
                    except (json.JSONDecodeError, OSError):
                        meta = {}
                parts = self.parts(name)
                samples = 0
                t_start = t_end = None
                for part in parts:
                    try:
                        data = np.load(part)
                    except (OSError, ValueError):
                        continue
                    sh = data["t_us"].shape[0]
                    if sh:
                        lo, hi = float(data["t_us"][0]), float(data["t_us"][-1])
                        t_start = lo if t_start is None else min(t_start, lo)
                        t_end = hi if t_end is None else max(t_end, hi)
                    samples += sh
                sessions.append({
                    "name": name,
                    "samples": samples,
                    "t_start_us": t_start,
                    "t_end_us": t_end,
                    "parts": len(parts),
                    "meta": meta,
                })
            return sessions

    def load(self, name, kinds=None):
        """Return ``(t_us, current_ma, kind)`` or raise KeyError.

        ``kinds`` optionally filters to a subset like ``{b'stream'}``.
        """
        with self._lock:
            parts = self.parts(name)
            if not parts:
                raise KeyError(f"no session '{name}'")
            arrays = []
            for part in parts:
                try:
                    data = np.load(part)
                except (OSError, ValueError):
                    continue
                t = data["t_us"]
                c = data["current_ma"]
                k = data["kind"]
                if kinds is not None:
                    keep = np.isin(k, np.array(list(kinds), dtype="S8"))
                    t, c, k = t[keep], c[keep], k[keep]
                arrays.append((t, c, k))
            return (np.concatenate([a[0] for a in arrays]),
                    np.concatenate([a[1] for a in arrays]),
                    np.concatenate([a[2] for a in arrays]))

    def count(self, name):
        with self._lock:
            total = 0
            for part in self.parts(name):
                try:
                    total += int(np.load(part)["t_us"].shape[0])
                except (OSError, ValueError):
                    continue
            return total

    def delete(self, name):
        with self._lock:
            removed = [p for p in self.log_dir.glob(f"{name}.*") if not p.is_dir()]
            for p in removed:
                p.unlink()
            return bool(removed)