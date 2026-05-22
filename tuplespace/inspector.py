"""
 Inspector Interface

Attach an Inspector to any TupleSpace for live observability:
  inspector.snapshot()           — formatted current state
  inspector.tail(n)              — last N log events
  inspector.filter_tuples(type_) — tuples of a given type
  inspector.run(refresh)         — blocking live terminal display (Ctrl-C to exit)

Also usable as:
  python -m tuplespace.inspector <path/to/events.jsonl>
"""

import json
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import TupleSpace


_TYPE_WIDTH = 20
_ID_WIDTH   = 8


def _fmt_tuple(t: dict) -> str:
    tid    = t.get("id", "")[:_ID_WIDTH]
    ttype  = t.get("type", "")[:_TYPE_WIDTH]
    dur    = t.get("durability", "hard")[0]   # h / s
    ver    = t.get("version", 0)
    ttl    = t.get("ttl")
    payload = t.get("payload", {})
    ttl_str = f"  ttl={ttl:.0f}s" if ttl else ""
    return (
        f"  [{dur}] {tid}  {ttype:<{_TYPE_WIDTH}}  v{ver:<3d}  "
        f"{str(payload)[:60]}{ttl_str}"
    )


def _fmt_event(e: dict) -> str:
    ts    = e.get("timestamp", 0)
    op    = e.get("op", "")
    t     = e.get("tuple") or {}
    ttype = t.get("type", "")[:_TYPE_WIDTH]
    tid   = (t.get("id") or "")[:_ID_WIDTH]
    return f"  {ts:.3f}  {op:<16s}  {ttype:<{_TYPE_WIDTH}}  {tid}"


class Inspector:
    def __init__(self, ts: "TupleSpace") -> None:
        self._ts = ts

    #  read-only view

    def snapshot(self, type_filter: str | None = None) -> str:
        pattern = {"type": type_filter} if type_filter else {"type": "*"}
        tuples = self._ts.query(pattern)
        by_type: dict[str, list[dict]] = {}
        for t in tuples:
            by_type.setdefault(t["type"], []).append(t)

        lines = [f"=== Tuple Space Snapshot  ({len(tuples)} tuples) ==="]
        for ttype in sorted(by_type):
            lines.append(f"\n  -- {ttype} ({len(by_type[ttype])}) --")
            for t in by_type[ttype]:
                lines.append(_fmt_tuple(t))
        return "\n".join(lines)

    def tail(self, n: int = 20, type_filter: str | None = None) -> str:
        entries = self._ts.log_entries()
        if type_filter:
            entries = [
                e for e in entries
                if (e.get("tuple") or {}).get("type") == type_filter
            ]
        recent = entries[-n:]
        lines = [f"=== Last {len(recent)} log events ==="]
        lines += [_fmt_event(e) for e in recent]
        return "\n".join(lines)

    def filter_tuples(self, type_: str) -> list[dict]:
        return self._ts.query({"type": type_})

    def stats(self) -> dict:
        tuples  = self._ts.query({"type": "*"})
        entries = self._ts.log_entries()
        by_type: dict[str, int] = {}
        for t in tuples:
            by_type[t["type"]] = by_type.get(t["type"], 0) + 1
        op_counts: dict[str, int] = {}
        for e in entries:
            op_counts[e["op"]] = op_counts.get(e["op"], 0) + 1
        return {
            "live_tuples": len(tuples),
            "total_events": len(entries),
            "by_type": by_type,
            "op_counts": op_counts,
        }

    #  live display 

    def run(self, refresh: float = 1.0, type_filter: str | None = None) -> None:
        """Blocking live terminal display. Ctrl-C to exit."""
        try:
            while True:
                
                print("\033[H\033[J", end="")
                print(self.snapshot(type_filter))
                print()
                print(self.tail(10, type_filter))
                print()
                s = self.stats()
                print(f"  events={s['total_events']}  live={s['live_tuples']}"
                      f"  ops={s['op_counts']}")
                print(f"\n  [refresh every {refresh}s — Ctrl-C to exit]")
                time.sleep(refresh)
        except KeyboardInterrupt:
            pass


#  standalone: tail a .jsonl log file

def _tail_file(path: Path, n: int = 40) -> None:
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    with path.open(encoding="utf-8") as f:
        entries = [json.loads(line) for line in f if line.strip()]

    print(f"=== {path}  ({len(entries)} events) ===\n")
    for e in entries[-n:]:
        print(_fmt_event(e))

    print("\n=== Final state (replay) ===")
    from .replay import replay
    state = replay(entries)
    by_type: dict[str, list] = {}
    for t in state:
        by_type.setdefault(t["type"], []).append(t)
    for ttype in sorted(by_type):
        print(f"\n  -- {ttype} ({len(by_type[ttype])}) --")
        for t in by_type[ttype]:
            print(_fmt_tuple(t))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m tuplespace.inspector <events.jsonl> [n_tail]")
        sys.exit(1)
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 40
    _tail_file(Path(sys.argv[1]), n)
