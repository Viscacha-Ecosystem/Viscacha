"""
Viscacha Dashboard — live terminal view of jobs and tuple space state.

Wrap any code with the context manager and the dashboard runs alongside it:

    from viscacha.dashboard import Dashboard

    with Dashboard(client):
        worker.run(blocking=False)
        for h in handles:
            h.wait()
    # dashboard stays open until the block exits, then prints a final snapshot

Standalone against an HTTP server or log file:
    python -m viscacha.dashboard --url http://localhost:8000
    python -m viscacha.dashboard --log jobs.jsonl
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:
    raise ImportError("pip install rich  to use the dashboard")

if TYPE_CHECKING:
    from .client import Client

BAR_WIDTH = 32
STATUS_COLOR = {
    "done":      "green",
    "failed":    "red",
    "pending":   "yellow",
    "cancelled": "dim",
}


def _ts(epoch: float | None) -> str:
    if epoch is None:
        return "--:--:--.---"
    return datetime.fromtimestamp(epoch).strftime("%H:%M:%S.%f")[:-3]


def _dur(start: float | None, end: float | None) -> str:
    if start is None:
        return "--"
    return f"{(end or time.time()) - start:.2f}s"


def _gantt_bar(enqueued: float | None, started: float | None,
               finished: float | None, t_min: float, t_max: float) -> Text:
    span = max(t_max - t_min, 0.001)
    now  = time.time()

    def col(t: float | None, fallback: float) -> int:
        return max(0, min(BAR_WIDTH, int(((t or fallback) - t_min) / span * BAR_WIDTH)))

    eq = col(enqueued, t_min)
    st = col(started,  now)
    ft = col(finished, now)

    bar = Text()
    bar.append(" " * eq)
    if started is not None:
        bar.append("-" * max(0, st - eq), style="dim yellow")  # queued wait
    bar.append("#" * max(0, ft - st),
               style="green" if finished else "cyan")          # executing
    bar.append(" " * max(0, BAR_WIDTH - max(ft, st)))
    return bar


class Dashboard:
    """
    Context manager — wraps any block of code with a live terminal dashboard.

        with Dashboard(client):
            worker.run(blocking=False)
            handle.wait()
    """

    def __init__(self, client: "Client", refresh: float = 0.5):
        self._client  = client
        self._refresh = refresh
        self._console = Console(legacy_windows=False)
        self._stop    = threading.Event()
        self._thread: threading.Thread | None = None

    # ── context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "Dashboard":
        self._stop.clear()
        self._thread = threading.Thread(target=self._live_loop, daemon=True, name="dashboard")
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)
        # final snapshot printed to normal console after live screen clears
        self._console.print(self._build())

    # ── blocking / standalone mode ────────────────────────────────────────

    def run(self, once: bool = False) -> None:
        """Run the dashboard directly (not as a context manager)."""
        if once:
            self._console.print(self._build())
            return
        with Live(self._build(), refresh_per_second=int(1 / self._refresh),
                  console=self._console, screen=True) as live:
            try:
                while True:
                    time.sleep(self._refresh)
                    live.update(self._build())
            except KeyboardInterrupt:
                pass

    # ── internals ─────────────────────────────────────────────────────────

    def _live_loop(self) -> None:
        with Live(self._build(), refresh_per_second=int(1 / self._refresh),
                  console=self._console, screen=True) as live:
            while not self._stop.is_set():
                time.sleep(self._refresh)
                live.update(self._build())

    def _build(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(self._header(), name="header", size=3),
            Layout(name="body",    ratio=1),
            Layout(self._log(),    name="log",    size=12),
        )
        layout["body"].split_row(
            Layout(self._gantt(),  name="gantt",  ratio=3),
            Layout(name="right",   ratio=2),
        )
        layout["right"].split_column(
            Layout(self._space(),  name="space",  ratio=2),
            Layout(self._leases(), name="leases", ratio=1),
        )
        return layout

    def _header(self) -> Panel:
        jobs   = self._client.jobs()
        counts = {s: sum(1 for j in jobs if j.status == s)
                  for s in ("done", "failed", "pending", "cancelled")}
        parts  = [
            f"[{STATUS_COLOR[s]}]{n} {s}[/{STATUS_COLOR[s]}]"
            for s, n in counts.items()
        ]
        now = datetime.now().strftime("%H:%M:%S")
        return Panel(
            Text.from_markup(
                f"[bold cyan]Viscacha[/bold cyan]   {'  |  '.join(parts)}   [dim]{now}[/dim]"
            ),
            style="bold",
        )

    def _gantt(self) -> Panel:
        jobs = self._client.jobs()
        if not jobs:
            return Panel("[dim]no jobs yet[/dim]", title="Timeline")

        times = [j.enqueued_at for j in jobs if j.enqueued_at]
        ends  = [j.finished_at for j in jobs if j.finished_at]
        t_min = min(times) if times else time.time()
        t_max = max(ends)  if ends  else time.time()
        if t_max <= t_min:
            t_max = t_min + 1

        table = Table(box=None, padding=(0, 1), show_header=False, expand=True)
        table.add_column("job",  style="bold", min_width=14, max_width=20)
        table.add_column("bar",  min_width=BAR_WIDTH, max_width=BAR_WIDTH)
        table.add_column("dur",  justify="right", width=7)
        table.add_column("st",   width=2)

        icons = {"done": "[green]v[/green]", "failed": "[red]x[/red]",
                 "pending": "[yellow].[/yellow]", "cancelled": "[dim]-[/dim]"}

        for job in sorted(jobs, key=lambda j: j.enqueued_at or 0):
            bar   = _gantt_bar(job.enqueued_at, job.started_at, job.finished_at, t_min, t_max)
            label = job.job_type[:16]
            if job.args:
                hint  = str(next(iter(job.args.values())))[:8]
                label = f"{label}({hint})"
            table.add_row(
                label[:20],
                bar,
                f"[dim]{_dur(job.started_at, job.finished_at)}[/dim]",
                Text.from_markup(icons.get(job.status, "?")),
            )

        return Panel(table, title="[bold]Timeline[/bold]")

    def _space(self) -> Panel:
        space = getattr(self._client, "_space", None)
        if space is None:
            return Panel("[dim]not available in remote mode[/dim]", title="Tuple Space")

        counts: dict[str, int] = {}
        for t in space.query({}):
            ttype = t.get("type", "?")
            counts[ttype] = counts.get(ttype, 0) + 1

        table = Table(box=None, padding=(0, 1), show_header=False, expand=True)
        table.add_column("type",  style="cyan")
        table.add_column("n",     justify="right", style="bold")

        for ttype, n in sorted(counts.items()):
            table.add_row(ttype, str(n))
        if not counts:
            table.add_row("[dim]empty[/dim]", "")

        return Panel(table, title=f"[bold]Tuple Space[/bold]  [dim]{sum(counts.values())} tuples[/dim]")

    def _leases(self) -> Panel:
        space  = getattr(self._client, "_space", None)
        leases = getattr(space, "_leases", {}) if space else {}

        table = Table(box=None, padding=(0, 1), show_header=False, expand=True)
        table.add_column("id",  style="dim",  max_width=10)
        table.add_column("ttl", justify="right", width=6)

        for lease_id, info in leases.items():
            remaining = info["expires_at"] - time.time()
            color     = "green" if remaining > 5 else "yellow" if remaining > 0 else "red"
            ttype     = info.get("tuple", {}).get("type", "?")[:20]
            table.add_row(f"{lease_id[:8]}  {ttype}", f"[{color}]{remaining:.0f}s[/{color}]")
        if not leases:
            table.add_row("[dim]none[/dim]", "")

        return Panel(table, title="[bold]Active Leases[/bold]")

    def _log(self) -> Panel:
        space = getattr(self._client, "_space", None)
        if space is None:
            return Panel("[dim]log not available in remote mode[/dim]", title="Event Log")

        entries = list(getattr(space._log, "_entries", []))[-20:]

        OP_COLOR = {
            "out": "green", "in": "red", "cas_new": "green", "cas_old": "red",
            "lease_acquire": "yellow", "lease_confirm": "green",
            "lease_release": "dim",    "lease_expire": "red", "expire": "dim red",
        }

        table = Table(box=None, padding=(0, 1), show_header=False, expand=True)
        table.add_column("time", style="dim",       width=13)
        table.add_column("op",   style="bold cyan", width=16)
        table.add_column("type", style="cyan",      width=24)
        table.add_column("info", style="dim")

        for e in entries:
            op    = e.get("op", "?")
            color = OP_COLOR.get(op, "white")
            ttype = (e.get("tuple") or {}).get("type", "")
            info  = e.get("agent_id") or ""
            table.add_row(_ts(e.get("timestamp")), f"[{color}]{op}[/{color}]", ttype, info)

        return Panel(table, title="[bold]Event Log[/bold]")


# ── __main__ ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))

    parser = argparse.ArgumentParser(description="Viscacha dashboard")
    parser.add_argument("--url",  help="HTTP server URL")
    parser.add_argument("--log",  help="Path to jobs.jsonl")
    parser.add_argument("--once", action="store_true", help="Snapshot and exit")
    args = parser.parse_args()

    from viscacha import Client
    if args.url:
        c = Client(url=args.url)
    elif args.log:
        c = Client(log_path=args.log)
    else:
        print("Usage: python -m viscacha.dashboard --url http://localhost:8000")
        print("       python -m viscacha.dashboard --log jobs.jsonl")
        sys.exit(1)

    Dashboard(c).run(once=args.once)
