"""Host-side threshold/edge detection on the live MetaShunt stream.

The MetaShunt has native burst triggering, but a firmware-testing workflow also
wants to detect crossings in the *continuous* stream (e.g. a wakeup current
spike) and be notified / record the moment.  This module provides a small,
stateless-per-trigger edge detector.

Edge semantics
--------------
A trigger has ``above_ma`` and/or ``below_ma`` bounds.  A **rising** event is
recorded the first time current crosses from at/below ``above_ma`` to above it;
a **falling** event when crossing an ``below_ma`` bound.  The detector tracks
state so it does not re-fire on every sample while already past the threshold.
"""

import threading


class TriggerRule:
    __slots__ = ("name", "above_ma", "below_ma", "pre_samples",
                 "post_samples", "active", "events")

    def __init__(self, name, above_ma=None, below_ma=None,
                 pre_samples=0, post_samples=0, active=True):
        self.name = name
        self.above_ma = above_ma
        self.below_ma = below_ma
        self.pre_samples = int(pre_samples)
        self.post_samples = int(post_samples)
        self.active = active
        #: fired events: list of (index_in_feed, rising_bool, t_us, cur_ma)
        self.events = []


class TriggerManager:
    """Evaluates a set of :class:`TriggerRule` against a fed sample stream."""

    def __init__(self):
        self._rules = {}
        self._state = {}  # name -> dict of above/below crossed flags
        self._lock = threading.Lock()
        self._feed_idx = 0

    def add_rule(self, name, above_ma=None, below_ma=None,
                 pre_samples=0, post_samples=0):
        rule = TriggerRule(name, above_ma, below_ma, pre_samples, post_samples)
        with self._lock:
            self._rules[name] = rule
            self._state[name] = {"above": above_ma is None, "below": below_ma is None}

    def remove_rule(self, name):
        with self._lock:
            self._rules.pop(name, None)
            self._state.pop(name, None)

    def list_rules(self):
        with self._lock:
            return {
                name: {"above_ma": r.above_ma, "below_ma": r.below_ma,
                       "active": r.active}
                for name, r in self._rules.items()
            }

    def feed(self, t_us, cur_ma):
        """Feed a decimated ``(t,cur)`` pair; return newly fired events."""
        fired = []
        with self._lock:
            i = self._feed_idx
            self._feed_idx += 1
            for name, rule in self._rules.items():
                if not rule.active or (rule.above_ma is None and rule.below_ma is None):
                    continue
                st = self._state[name]
                if rule.above_ma is not None:
                    if cur_ma > rule.above_ma and not st["above"]:
                        st["above"] = True
                        rule.events.append((i, True, t_us, cur_ma))
                        fired.append((name, i, True, t_us, cur_ma))
                    elif cur_ma <= rule.above_ma and st["above"]:
                        st["above"] = False
                if rule.below_ma is not None:
                    if cur_ma < rule.below_ma and not st["below"]:
                        st["below"] = True
                        rule.events.append((i, False, t_us, cur_ma))
                        fired.append((name, i, False, t_us, cur_ma))
                    elif cur_ma >= rule.below_ma and st["below"]:
                        st["below"] = False
        return fired

    def clear(self):
        with self._lock:
            for r in self._rules.values():
                r.events.clear()
            for st in self._state.values():
                st["above"] = st["above"] if "above" in st else True
            self._feed_idx = 0