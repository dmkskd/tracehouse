"""TraceHouse TUI — orchestrates data-utils tools with shared test users.

Creates test users once, then launches tool subprocesses with credentials
injected via TRACEHOUSE_TEST_USERS env var. Provides a dashboard to
start/stop tools and monitor their output.

Usage:
    tracehouse-data-tools-tui [options]
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

from rich.markup import escape as rich_escape
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import (
    Footer, Header, Label, RichLog, Static, TabbedContent, TabPane,
    TextArea,
)

# Regex to strip ANSI escape sequences from piped output
_ANSI_RE = re.compile(r"\033\[[0-9;]*[A-Za-z]")
# Detect progress-bar lines (contain [████░░░─] patterns)
_PROGRESS_RE = re.compile(r"\[[\u2588\u2591\u2500]{10,}\]")

from data_utils.env import (
    add_connection_args, make_client, pre_parse_env_file,
    print_connection,
)
from data_utils.users import (
    create_test_users, print_test_users, serialize_test_users,
    TestUser,
)


# ── Tool definitions ───────────────────────────────────────────────

TOOL_DEFS = {
    "generate":       "data_utils.cli.generate",
    "queries":        "data_utils.cli.queries",
    "mutations":      "data_utils.cli.mutations",
    "merge-triggers": "data_utils.cli.merge_triggers",
}

# Colors assigned to each tool for log prefixes
TOOL_COLORS = {
    "generate":       "#6bcb77",
    "queries":        "#4d96ff",
    "mutations":      "#ff6b6b",
    "merge-triggers": "#ffd93d",
}


@dataclass
class ManagedTool:
    """A subprocess managed by the TUI."""
    name: str
    module: str
    extra_args: list[str] = field(default_factory=list)
    proc: subprocess.Popen | None = None
    started_at: datetime | None = None

    @property
    def status(self) -> str:
        if self.proc is None:
            return "stopped"
        rc = self.proc.poll()
        if rc is None:
            return "running"
        return f"exited ({rc})"

    @property
    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


# ── App ────────────────────────────────────────────────────────────

class TraceHouseTUI(App):
    """TUI for managing TraceHouse data tools."""

    TITLE = "TraceHouse"
    SUB_TITLE = "Data Tools Manager"

    CSS = """
    Screen {
        background: $surface;
    }

    #header-bar {
        height: 1;
        padding: 0 1;
        background: $boost;
        color: $text-muted;
        margin: 0 0 1 0;
    }

    #tools-bar {
        height: 1;
        padding: 0 1;
        background: $panel;
        margin: 0;
    }

    .tool-status-item {
        width: auto;
        padding: 0 1;
    }

    #progress-area {
        height: auto;
        max-height: 8;
        padding: 0 2;
        background: $panel;
        color: $text-muted;
        margin: 0 0 1 0;
    }

    #progress-area.--empty {
        display: none;
    }

    #tools-bar.--no-progress {
        margin: 0 0 1 0;
    }

    #log-tabs {
        height: 1fr;
        margin: 0 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("a", "start_all", "Start All"),
        ("s", "stop_all", "Stop All"),
        ("1", "toggle_tool('generate')", "Toggle generate"),
        ("2", "toggle_tool('queries')", "Toggle queries"),
        ("3", "toggle_tool('mutations')", "Toggle mutations"),
        ("4", "toggle_tool('merge-triggers')", "Toggle merge-triggers"),
        ("c", "copy_log", "Copy Log"),
        ("x", "clear_log", "Clear Log"),
        ("ctrl+s", "save_env", "Save .env"),
    ]

    def __init__(
        self,
        users: list[TestUser],
        child_env: dict[str, str],
        tool_extra_args: dict[str, str],
        env_path: str | None = None,
    ) -> None:
        super().__init__()
        self.users = users
        self.child_env = child_env
        self.env_path = env_path
        self.tools: dict[str, ManagedTool] = {}
        for name, module in TOOL_DEFS.items():
            extra = tool_extra_args.get(name, "")
            args = extra.split() if extra else []
            self.tools[name] = ManagedTool(name=name, module=module, extra_args=args)
        # Plain-text log buffers for clipboard copy
        self._log_lines: dict[str, list[str]] = {"all": []}
        for name in TOOL_DEFS:
            self._log_lines[name] = []
        # Per-tool progress lines: tool_name -> {dataset_name: display_line}
        self._progress: dict[str, dict[str, str]] = {name: {} for name in TOOL_DEFS}

    def compose(self) -> ComposeResult:
        yield Header()

        # Connection info — single line
        env_label = os.path.basename(self.env_path) if self.env_path else "(no .env)"
        host = self.child_env.get("CH_HOST", "localhost")
        port = self.child_env.get("CH_PORT", "9000")
        user = self.child_env.get("CH_USER", "default")
        user_info = f"{len(self.users)} test users" if self.users else user
        yield Static(
            f" Generate realistic ClickHouse workloads: queries, mutations, merges, and bulk loads.  "
            f"[dim]{env_label}  {host}:{port}  {user_info}[/]",
            id="header-bar",
        )

        # Tool status — compact single line with numbered shortcuts
        tool_items = []
        for i, name in enumerate(self.tools, 1):
            color = TOOL_COLORS.get(name, "white")
            tool_items.append(f"[dim]{i}:[/][{color}]{name}[/] [dim]stopped[/]")
        with Horizontal(id="tools-bar", classes="--no-progress"):
            for i, name in enumerate(self.tools, 1):
                color = TOOL_COLORS.get(name, "white")
                yield Label(
                    f"[dim]{i}:[/][{color} bold]{name}[/] [dim]stopped[/]",
                    id=f"status-{name}",
                    classes="tool-status-item",
                )

        # Live progress area (updated in-place for progress-bar output, hidden when empty)
        yield Static("", id="progress-area", classes="--empty")

        # Tabbed logs — one tab per tool + an "All" tab + env file
        with TabbedContent(id="log-tabs"):
            with TabPane("All", id="tab-all"):
                yield RichLog(id="log-all", highlight=True, markup=True)
            for name in self.tools:
                color = TOOL_COLORS.get(name, "white")
                with TabPane(name, id=f"tab-{name}"):
                    yield RichLog(id=f"log-{name}", highlight=True, markup=True)
            with TabPane(".env", id="tab-env"):
                env_text = ""
                if self.env_path and os.path.isfile(self.env_path):
                    with open(self.env_path) as f:
                        env_text = f.read()
                yield TextArea(
                    env_text,
                    id="env-editor",
                    language="bash",
                    show_line_numbers=True,
                    read_only=not bool(self.env_path),
                    tab_behavior="indent",
                )

        yield Footer()

    def on_mount(self) -> None:
        self._refresh_timer = self.set_interval(1, self._refresh_statuses)

    # ── Logging ────────────────────────────────────────────────────

    def _log(self, tool_name: str, line: str) -> None:
        """Write a line to the tool's log tab and the All tab."""
        color = TOOL_COLORS.get(tool_name, "white")
        ts = datetime.now().strftime("%H:%M:%S")
        prefix = f"[dim]{ts}[/dim] [{color}]{tool_name:>15}[/] "

        # Strip Rich markup for plain-text buffer
        plain = f"{ts} {tool_name:>15}  {line}"
        self._log_lines["all"].append(plain)
        self._log_lines.setdefault(tool_name, []).append(f"{ts}  {line}")

        all_log = self.query_one("#log-all", RichLog)
        all_log.write(prefix + rich_escape(line))

        try:
            tool_log = self.query_one(f"#log-{tool_name}", RichLog)
            tool_log.write(f"[dim]{ts}[/dim] {rich_escape(line)}")
        except Exception:
            pass

    def _log_system(self, msg: str) -> None:
        """Write a system message to the All tab."""
        ts = datetime.now().strftime("%H:%M:%S")
        plain = f"{ts} {'system':>15}  {msg}"
        self._log_lines["all"].append(plain)

        all_log = self.query_one("#log-all", RichLog)
        all_log.write(f"[dim]{ts}[/dim] [bold dim]{'system':>15}[/] {msg}")

    # ── Progress area ──────────────────────────────────────────────

    def _update_progress(self, tool_name: str, line: str) -> None:
        """Parse a progress-bar line and update the live progress area."""
        # Extract dataset name (second whitespace-delimited token after emoji)
        parts = line.split()
        if len(parts) >= 2:
            dataset = parts[1]  # e.g. "synthetic_data"
        else:
            dataset = "unknown"

        color = TOOL_COLORS.get(tool_name, "white")
        # Escape Rich markup so [████░░░░] brackets aren't parsed as tags
        self._progress[tool_name][dataset] = f"[{color}]{rich_escape(line.strip())}[/]"
        self._render_progress()

    def _clear_progress(self, tool_name: str) -> None:
        """Clear progress lines for a tool (e.g. when it exits)."""
        if self._progress.get(tool_name):
            self._progress[tool_name].clear()
            self._render_progress()

    def _render_progress(self) -> None:
        """Re-render the combined progress area from all tools."""
        lines = []
        for _tool, datasets in self._progress.items():
            for _ds, display in datasets.items():
                lines.append(display)
        area = self.query_one("#progress-area", Static)
        tools_bar = self.query_one("#tools-bar", Horizontal)
        if lines:
            area.update("\n".join(lines))
            area.remove_class("--empty")
            tools_bar.remove_class("--no-progress")
        else:
            area.update("")
            area.add_class("--empty")
            tools_bar.add_class("--no-progress")

    # ── Status refresh ─────────────────────────────────────────────

    def _refresh_statuses(self) -> None:
        for i, (name, tool) in enumerate(self.tools.items(), 1):
            label = self.query_one(f"#status-{name}", Label)
            color = TOOL_COLORS.get(name, "white")

            status = tool.status
            if status == "running" and tool.started_at:
                dt = datetime.now() - tool.started_at
                mins = int(dt.total_seconds()) // 60
                secs = int(dt.total_seconds()) % 60
                label.update(
                    f"[dim]{i}:[/][{color} bold]{name}[/] [green]●[/] [dim]{mins}m{secs:02d}s[/]"
                )
            elif status == "stopped":
                label.update(
                    f"[dim]{i}:[/][{color} bold]{name}[/] [dim]○[/]"
                )
            else:
                label.update(
                    f"[dim]{i}:[/][{color} bold]{name}[/] [red]✕ {status}[/]"
                )

    # ── Start / Stop ───────────────────────────────────────────────

    def _start_tool(self, tool: ManagedTool) -> None:
        if tool.is_running:
            self._log_system(f"[yellow]{tool.name} already running[/]")
            return

        cmd = [sys.executable, "-m", tool.module] + tool.extra_args
        try:
            tool.proc = subprocess.Popen(
                cmd,
                env=self.child_env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            tool.started_at = datetime.now()
            self._log_system(f"[green]Started[/] {tool.name} (pid {tool.proc.pid})")
            # Start background reader for this tool's stdout
            self._stream_output(tool)
        except Exception as e:
            self._log_system(f"[red]Failed to start {tool.name}:[/] {e}")

    @work(thread=True)
    def _stream_output(self, tool: ManagedTool) -> None:
        """Read subprocess stdout and handle \\r-based progress updates."""
        proc = tool.proc
        if proc is None or proc.stdout is None:
            return
        buf = b""
        try:
            while True:
                # Read one byte at a time to avoid blocking on partial lines.
                # With PYTHONUNBUFFERED=1, data arrives promptly.
                byte = proc.stdout.read(1)
                if not byte:
                    break
                if byte == b"\n":
                    line = self._resolve_cr(buf)
                    buf = b""
                    if not line:
                        continue
                    # Strip ANSI escape sequences (cursor movement, colors)
                    clean = _ANSI_RE.sub("", line).strip()
                    if not clean:
                        continue
                    # Route progress-bar lines to the live progress area
                    if _PROGRESS_RE.search(clean):
                        self.call_from_thread(self._update_progress, tool.name, clean)
                    else:
                        self.call_from_thread(self._log, tool.name, clean)
                else:
                    buf += byte
            # Flush remaining buffer
            if buf:
                line = self._resolve_cr(buf)
                if line:
                    clean = _ANSI_RE.sub("", line).strip()
                    if clean:
                        if _PROGRESS_RE.search(clean):
                            self.call_from_thread(self._update_progress, tool.name, clean)
                        else:
                            self.call_from_thread(self._log, tool.name, clean)
        except (ValueError, OSError):
            pass  # pipe closed
        # Process ended — clear progress and log exit
        self.call_from_thread(self._clear_progress, tool.name)
        rc = proc.wait()
        self.call_from_thread(
            self._log_system,
            f"[dim]{tool.name} exited (code {rc})[/]",
        )

    @staticmethod
    def _resolve_cr(raw: bytes) -> str:
        """Simulate terminal \\r: keep only the last carriage-return segment."""
        text = raw.decode("utf-8", errors="replace")
        if "\r" in text:
            text = text.rsplit("\r", 1)[-1]
        return text.rstrip()

    def _stop_tool(self, tool: ManagedTool) -> None:
        if not tool.is_running:
            return

        self._log_system(f"Stopping {tool.name}...")
        try:
            os.killpg(os.getpgid(tool.proc.pid), signal.SIGINT)
        except (ProcessLookupError, OSError):
            pass
        # The _stream_output worker will detect the exit and log it


    # ── Actions ────────────────────────────────────────────────────

    def action_toggle_tool(self, name: str) -> None:
        tool = self.tools.get(name)
        if not tool:
            return
        if tool.is_running:
            self._stop_tool(tool)
        else:
            self._start_tool(tool)

    def action_start_all(self) -> None:
        for tool in self.tools.values():
            self._start_tool(tool)

    def action_stop_all(self) -> None:
        for tool in self.tools.values():
            self._stop_tool(tool)

    def action_clear_log(self) -> None:
        """Clear the active tab's log."""
        tabs = self.query_one("#log-tabs", TabbedContent)
        active_id = tabs.active
        key = active_id.removeprefix("tab-") if active_id else "all"
        try:
            log_widget = self.query_one(f"#log-{key}", RichLog)
            log_widget.clear()
        except Exception:
            pass
        if key in self._log_lines:
            self._log_lines[key].clear()

    def action_save_env(self) -> None:
        """Save the .env editor contents back to disk."""
        if not self.env_path:
            self._log_system("[yellow]No .env file to save[/]")
            return
        try:
            editor = self.query_one("#env-editor", TextArea)
            with open(self.env_path, "w") as f:
                f.write(editor.text)
            self._log_system(f"[green]Saved {self.env_path}[/]")
        except Exception as e:
            self._log_system(f"[red]Failed to save .env:[/] {e}")

    def action_copy_log(self) -> None:
        """Copy the active tab's log to the clipboard."""
        tabs = self.query_one("#log-tabs", TabbedContent)
        active_id = tabs.active  # e.g. "tab-all" or "tab-queries"
        key = active_id.removeprefix("tab-") if active_id else "all"
        lines = self._log_lines.get(key, self._log_lines["all"])
        text = "\n".join(lines)
        self.copy_to_clipboard(text)
        n = len(lines)
        self._log_system(f"[green]Copied {n} lines from '{key}' to clipboard[/]")

    def on_unmount(self) -> None:
        """Clean up all subprocesses on exit."""
        for tool in self.tools.values():
            if tool.is_running:
                try:
                    os.killpg(os.getpgid(tool.proc.pid), signal.SIGINT)
                except (ProcessLookupError, OSError):
                    pass
        deadline = time.time() + 3
        for tool in self.tools.values():
            if tool.proc and tool.proc.poll() is None:
                remaining = max(0, deadline - time.time())
                try:
                    tool.proc.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(tool.proc.pid), signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        pass


# ── CLI entry point ────────────────────────────────────────────────

def _parse_args():
    import argparse

    env_path = pre_parse_env_file()

    parser = argparse.ArgumentParser(
        description="TraceHouse TUI — orchestrate data tools with shared test users",
    )
    add_connection_args(parser)
    for name in TOOL_DEFS:
        flag = f"--{name}-args"
        parser.add_argument(flag, default="", help=f"Extra arguments for {name}")
    args = parser.parse_args()
    return args, env_path


def main() -> None:
    args, env_path = _parse_args()

    print_connection(args, env_path)

    # Optionally create test users
    test_users: list[TestUser] = []
    if args.users > 0:
        print(f"\nConnecting to {args.host}:{args.port}...")
        client = make_client(args)
        print(f"Creating {args.users} test users...")
        test_users = create_test_users(client, args.users)
        print_test_users(test_users)
        client.disconnect()

    # Build child env
    child_env = os.environ.copy()
    if test_users:
        child_env["TRACEHOUSE_TEST_USERS"] = serialize_test_users(test_users)
    child_env["CH_ASSUME_YES"] = "1"
    child_env["PYTHONUNBUFFERED"] = "1"
    # Forward connection settings (including env file so children load the same one)
    if env_path:
        child_env["CH_ENV_FILE"] = env_path
    child_env["CH_HOST"] = args.host
    child_env["CH_PORT"] = str(args.port)
    child_env["CH_USER"] = args.user
    child_env["CH_PASSWORD"] = args.password
    child_env["CH_SECURE"] = "1" if args.secure else ""
    child_env["CH_USER_SKEW"] = str(args.user_skew)

    # Collect per-tool extra args
    tool_extra_args = {}
    for name in TOOL_DEFS:
        key = name.replace("-", "_") + "_args"
        val = getattr(args, key, "")
        if val:
            tool_extra_args[name] = val

    # Launch TUI
    app = TraceHouseTUI(
        users=test_users,
        child_env=child_env,
        tool_extra_args=tool_extra_args,
        env_path=env_path,
    )
    app.run()


if __name__ == "__main__":
    main()
