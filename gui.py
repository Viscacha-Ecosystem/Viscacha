"""
Viscacha GUI Dashboard — lightweight Tkinter live view.

    from viscacha.gui import GUIDashboard

    with GUIDashboard(client):
        worker.run(blocking=False)
        for h in handles:
            h.wait()

    # window stays open after the block; call .wait() to block until user closes it
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING

import tkinter as tk

if TYPE_CHECKING:
    from .client import Client

# ── theme ─────────────────────────────────────────────────────────────────────

BG        = "#f4f4f5"
PANEL     = "#ffffff"
BORDER    = "#e4e4e7"
TEXT      = "#18181b"
TEXT_DIM  = "#71717a"
HDR_BG    = "#18181b"
LOG_BG    = "#1e1e2e"
LOG_FG    = "#cdd6f4"

COLOR = {
    "done":      "#22c55e",
    "failed":    "#ef4444",
    "pending":   "#f59e0b",
    "cancelled": "#a1a1aa",
    "running":   "#3b82f6",
    "wait":      "#fef9c3",
}
BADGE = {
    "done":      ("#166534", "#dcfce7"),
    "failed":    ("#991b1b", "#fee2e2"),
    "pending":   ("#92400e", "#fef3c7"),
    "cancelled": ("#3f3f46", "#f4f4f5"),
}
LOG_TAG = {
    "out":     "#a6e3a1",
    "cas_new": "#a6e3a1",
    "in":      "#f38ba8",
    "cas_old": "#f38ba8",
    "expire":  "#f38ba8",
    "lease_acquire": "#f9e2af",
    "lease_confirm": "#89b4fa",
    "lease_release": "#585b70",
    "lease_expire":  "#585b70",
}


# ── widget helpers ────────────────────────────────────────────────────────────

def _panel(parent, **kw) -> tk.Frame:
    return tk.Frame(parent, bg=PANEL,
                    highlightthickness=1, highlightbackground=BORDER, **kw)


def _label(parent, text="", dim=False, bold=False, size=10, **kw) -> tk.Label:
    style = "bold" if bold else "normal"
    color = TEXT_DIM if dim else TEXT
    return tk.Label(parent, text=text, bg=kw.pop("bg", PANEL),
                    fg=color, font=("Segoe UI", size, style), **kw)


def _ro_text(parent, height=8, dark=False, **kw) -> tk.Text:
    bg = LOG_BG if dark else PANEL
    fg = LOG_FG if dark else TEXT
    t  = tk.Text(parent, bg=bg, fg=fg, font=("Consolas", 9),
                 state="disabled", relief="flat", height=height,
                 insertbackground=fg, selectbackground="#313244", **kw)
    return t


def _set(widget: tk.Text, content: str) -> None:
    widget.configure(state="normal")
    widget.delete("1.0", "end")
    widget.insert("end", content)
    widget.configure(state="disabled")


# ── dashboard ─────────────────────────────────────────────────────────────────

class GUIDashboard:
    REFRESH_MS = 400
    W, H       = 1060, 680

    def __init__(self, client: "Client", title: str = "Viscacha Dashboard"):
        self._client  = client
        self._title   = title
        self._done    = False
        self._closed  = threading.Event()
        self._thread: threading.Thread | None = None
        self._root:   tk.Tk | None            = None

    # ── context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "GUIDashboard":
        self._thread = threading.Thread(target=self._run_tk, daemon=True, name="gui-dashboard")
        self._thread.start()
        time.sleep(0.25)   # let window appear before work starts
        return self

    def __exit__(self, *_) -> None:
        self._done = True  # header turns green; window stays open

    def wait(self) -> None:
        """Block until the user closes the window."""
        self._closed.wait()

    # ── tk main loop (runs in background thread) ──────────────────────────

    def _run_tk(self) -> None:
        root = tk.Tk()
        self._root = root
        root.title(self._title)
        root.geometry(f"{self.W}x{self.H}")
        root.configure(bg=BG)
        root.resizable(True, True)
        root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build(root)
        root.after(self.REFRESH_MS, self._tick)
        root.mainloop()

    def _on_close(self) -> None:
        self._closed.set()
        if self._root:
            self._root.destroy()

    def _tick(self) -> None:
        try:
            self._refresh()
        except Exception:
            pass
        if self._root and self._root.winfo_exists():
            self._root.after(self.REFRESH_MS, self._tick)

    # ── layout ────────────────────────────────────────────────────────────

    def _build(self, root: tk.Tk) -> None:
        # ── header ────────────────────────────────────────────────────────
        hdr = tk.Frame(root, bg=HDR_BG, height=46)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        tk.Label(hdr, text="Viscacha", bg=HDR_BG, fg="#ffffff",
                 font=("Segoe UI", 13, "bold")).pack(side="left", padx=16, pady=12)

        self._badges   = tk.Frame(hdr, bg=HDR_BG)
        self._badges.pack(side="left", padx=6)
        self._hdr_note = tk.Label(hdr, text="", bg=HDR_BG, fg="#71717a",
                                  font=("Segoe UI", 10))
        self._hdr_note.pack(side="left", padx=8)
        self._clock    = tk.Label(hdr, text="", bg=HDR_BG, fg="#52525b",
                                  font=("Segoe UI", 10))
        self._clock.pack(side="right", padx=16)

        # ── body ──────────────────────────────────────────────────────────
        body = tk.Frame(root, bg=BG)
        body.pack(fill="both", expand=True, padx=8, pady=(8, 4))

        # timeline (left, expands)
        tl_panel = _panel(body)
        tl_panel.pack(side="left", fill="both", expand=True, padx=(0, 4))
        _label(tl_panel, "Timeline", dim=True, bold=True).pack(anchor="w", padx=10, pady=(8, 2))
        self._canvas = tk.Canvas(tl_panel, bg=PANEL, highlightthickness=0)
        self._canvas.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        # right column (fixed width)
        right = tk.Frame(body, bg=BG, width=270)
        right.pack(side="right", fill="y")
        right.pack_propagate(False)

        sp_panel = _panel(right)
        sp_panel.pack(fill="both", expand=True, pady=(0, 4))
        _label(sp_panel, "Tuple Space", dim=True, bold=True).pack(anchor="w", padx=10, pady=(8, 2))
        self._sp_text = _ro_text(sp_panel, height=12)
        self._sp_text.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        lz_panel = _panel(right)
        lz_panel.pack(fill="x")
        _label(lz_panel, "Active Leases", dim=True, bold=True).pack(anchor="w", padx=10, pady=(8, 2))
        self._lz_text = _ro_text(lz_panel, height=5)
        self._lz_text.pack(fill="x", padx=6, pady=(0, 6))

        # ── log ───────────────────────────────────────────────────────────
        log_panel = _panel(root)
        log_panel.pack(fill="x", padx=8, pady=(0, 8))
        _label(log_panel, "Event Log", dim=True, bold=True).pack(anchor="w", padx=10, pady=(8, 2))

        log_inner = tk.Frame(log_panel, bg=PANEL)
        log_inner.pack(fill="x", padx=6, pady=(0, 6))
        self._log_text = _ro_text(log_inner, height=8, dark=True)
        self._log_text.pack(side="left", fill="x", expand=True)
        sb = tk.Scrollbar(log_inner, command=self._log_text.yview)
        sb.pack(side="right", fill="y")
        self._log_text.configure(yscrollcommand=sb.set)

        for op, color in LOG_TAG.items():
            self._log_text.tag_configure(op, foreground=color)
        self._log_text.tag_configure("default", foreground=LOG_FG)
        self._log_text.tag_configure("dim",     foreground="#585b70")

    # ── refresh ───────────────────────────────────────────────────────────

    def _refresh(self) -> None:
        self._refresh_header()
        self._refresh_gantt()
        self._refresh_space()
        self._refresh_leases()
        self._refresh_log()

    def _refresh_header(self) -> None:
        for w in self._badges.winfo_children():
            w.destroy()

        jobs   = self._client.jobs()
        counts = {s: sum(1 for j in jobs if j.status == s)
                  for s in ("done", "failed", "pending", "cancelled")}

        for status, n in counts.items():
            if n == 0 and status in ("failed", "cancelled"):
                continue
            fg, bg = BADGE[status]
            tk.Label(self._badges, text=f"  {n} {status}  ", bg=bg, fg=fg,
                     font=("Segoe UI", 9, "bold"), padx=2).pack(side="left", padx=3)

        note = "  completed" if self._done else ""
        self._hdr_note.configure(text=note, fg="#4ade80" if self._done else "#71717a")
        self._clock.configure(text=datetime.now().strftime("%H:%M:%S"))

    def _refresh_gantt(self) -> None:
        cv = self._canvas
        cv.delete("all")

        jobs = self._client.jobs()
        if not jobs:
            cv.create_text(12, 20, text="No jobs yet", anchor="w",
                           fill=TEXT_DIM, font=("Segoe UI", 10))
            return

        jobs    = sorted(jobs, key=lambda j: j.enqueued_at or 0)
        eq_list = [j.enqueued_at for j in jobs if j.enqueued_at]
        ft_list = [j.finished_at for j in jobs if j.finished_at]
        t_min   = min(eq_list) if eq_list else time.time()
        t_max   = max(ft_list) if ft_list else time.time()
        if t_max <= t_min:
            t_max = t_min + 0.5

        W       = max(cv.winfo_width(),  500)
        H       = max(cv.winfo_height(), 200)
        LABEL_W = 170
        BAR_H   = max(14, min(28, (H - 40) // max(len(jobs), 1)))
        PAD_Y   = 10
        span    = t_max - t_min

        def tx(t: float) -> float:
            return LABEL_W + (t - t_min) / span * (W - LABEL_W - 14)

        for i, job in enumerate(jobs):
            y1 = PAD_Y + i * (BAR_H + 5)
            y2 = y1 + BAR_H
            yc = (y1 + y2) // 2

            # label
            label = job.job_type
            if job.args:
                hint  = str(next(iter(job.args.values())))[:12]
                label = f"{label}({hint})"
            cv.create_text(LABEL_W - 8, yc, text=label[:24], anchor="e",
                           fill=TEXT, font=("Segoe UI", 9))

            if not job.enqueued_at:
                continue

            now  = time.time()
            x_eq = tx(job.enqueued_at)
            x_st = tx(job.started_at)  if job.started_at  else tx(now)
            x_ft = tx(job.finished_at) if job.finished_at else tx(now)
            clr  = COLOR.get(job.status, COLOR["running"])

            # queue-wait band
            if x_st > x_eq + 1:
                cv.create_rectangle(x_eq, y1 + 4, x_st, y2 - 4,
                                    fill=COLOR["wait"], outline="")

            # execution bar
            x_end = max(x_ft, x_st + 4)
            cv.create_rectangle(x_st, y1, x_end, y2, fill=clr, outline="")

            # duration inside bar
            dur = (job.finished_at or now) - (job.started_at or job.enqueued_at)
            if x_end - x_st > 36:
                cv.create_text((x_st + x_end) / 2, yc, text=f"{dur:.2f}s",
                               fill="white", font=("Segoe UI", 8, "bold"))

        # time axis
        y_ax = PAD_Y + len(jobs) * (BAR_H + 5) + 6
        cv.create_line(LABEL_W, y_ax, W - 14, y_ax, fill=BORDER)
        for frac in (0, 0.25, 0.5, 0.75, 1.0):
            x = LABEL_W + frac * (W - LABEL_W - 14)
            cv.create_text(x, y_ax + 10, text=f"{span * frac:.1f}s",
                           fill=TEXT_DIM, font=("Segoe UI", 8))

    def _refresh_space(self) -> None:
        sp = getattr(self._client, "_space", None)
        if not sp:
            _set(self._sp_text, "  remote mode — not available")
            return
        counts: dict[str, int] = {}
        for t in sp.query({}):
            k = t.get("type", "?")
            counts[k] = counts.get(k, 0) + 1
        lines = [f"  {n:>4}  {k}" for k, n in sorted(counts.items())]
        _set(self._sp_text, "\n".join(lines) or "  (empty)")

    def _refresh_leases(self) -> None:
        sp     = getattr(self._client, "_space", None)
        leases = getattr(sp, "_leases", {}) if sp else {}
        lines  = []
        for lid, info in leases.items():
            rem   = info["expires_at"] - time.time()
            ttype = info.get("tuple", {}).get("type", "?")
            lines.append(f"  {lid[:8]}  {ttype:<20}  {rem:.0f}s")
        _set(self._lz_text, "\n".join(lines) or "  none")

    def _refresh_log(self) -> None:
        sp = getattr(self._client, "_space", None)
        if not sp:
            return
        entries = list(getattr(sp._log, "_entries", []))[-40:]

        self._log_text.configure(state="normal")
        self._log_text.delete("1.0", "end")
        for e in entries:
            ts    = datetime.fromtimestamp(e.get("timestamp", 0)).strftime("%H:%M:%S.%f")[:-3]
            op    = e.get("op", "?")
            ttype = (e.get("tuple") or {}).get("type", "")
            tag   = op if op in LOG_TAG else "default"
            self._log_text.insert("end", f"  {ts}  {op:<18}  {ttype}\n", tag)
        self._log_text.see("end")
        self._log_text.configure(state="disabled")
