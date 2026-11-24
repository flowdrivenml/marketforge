"""Exercise the real Textual terminal driver and editor supervision on two PTYs."""

import errno
import json
import os
import pty
import signal
import sys
import time
from pathlib import Path

import pytest

APP = """
import asyncio
import os
import shlex
import sys
import termios
from pathlib import Path

from textual.app import App
from textual.widgets import Static
from forge.shared.terminal.editor import EditorError, edit_file
from forge.shared.terminal.editor_window import edit_in_window

root = Path(sys.argv[1])
mode = sys.argv[2]
editor = shlex.join([sys.executable, str(root / "editor.py"), str(root)])
target = root / "target ü.toml"

class TerminalApp(App):
    def compose(self):
        yield Static("Parent TUI")

    def on_mount(self):
        self.run_worker(self.exercise(), exit_on_error=False)

    def on_key(self, event):
        if event.key in {"k", "j"}:
            (root / event.key).touch()
        if event.key == "q":
            self.exit()

    def on_click(self):
        (root / "mouse").touch()

    async def exercise(self):
        before = termios.tcgetattr(0)
        (root / "parent-tty").write_text(os.ttyname(0))
        try:
            if mode == "legacy":
                with self.suspend():
                    await asyncio.to_thread(edit_file, target, editor=editor)
            else:
                await edit_in_window(target, cwd=root, editor=editor)
        except EditorError as error:
            (root / "error").write_text(str(error))
        assert termios.tcgetattr(0) == before
        (root / "completed").touch()

try:
    TerminalApp().run()
except KeyboardInterrupt:
    (root / "parent-interrupted").touch()
    sys.exit(130)
"""

EDITOR = """
import json
import os
import sys
import time
from pathlib import Path

root = Path(sys.argv[1])
record = root / "editor-tty"
scratch = record.with_suffix(".tmp")
scratch.write_text(json.dumps({
    "pid": os.getpid(),
    "tty": [os.ttyname(fd) for fd in (0, 1, 2)],
    "session": os.getsid(0),
    "group": os.getpgrp(),
    "tmux": os.environ.get("TMUX"),
    "target": sys.argv[2],
}))
scratch.replace(record)
while not (root / "release").is_file():
    time.sleep(0.02)
sys.exit(int((root / "exit-status").read_text()))
"""

GHOSTTY = """
import errno
import os
import pty
import sys

args = sys.argv[1:]
if args[0] == "+show-config":
    for option in (
        "gtk-single-instance", "initial-window", "wait-after-command",
        "quit-after-last-window-closed", "abnormal-command-exit-runtime",
    ):
        print(f"{option} =")
    sys.exit()
if os.environ.get("FORGE_EDITOR_TEST_FAIL"):
    sys.exit(23)

pid, fd = pty.fork()
if pid == 0:
    argv = args[args.index("-e") + 1:]
    os.execv(argv[0], argv)
try:
    while os.read(fd, 4096):
        pass
except OSError as error:
    if error.errno != errno.EIO:
        raise
_, status = os.waitpid(pid, 0)
sys.exit(max(0, os.waitstatus_to_exitcode(status)))
"""


@pytest.fixture
def terminal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    (tmp_path / "app.py").write_text(APP)
    (tmp_path / "editor.py").write_text(EDITOR)
    fake_ghostty = tmp_path / "ghostty"
    fake_ghostty.write_text(f"#!{sys.executable}\n{GHOSTTY}")
    fake_ghostty.chmod(0o700)
    (tmp_path / "target ü.toml").write_text("version = 1\n")
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[4]))
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("DISPLAY", ":test")
    monkeypatch.setenv("TMUX", "unrelated-tmux")
    monkeypatch.delenv("TEXTUAL_ALLOW_SIGNALS", raising=False)
    return tmp_path


class Terminal:
    def __init__(self, root: Path, mode: str) -> None:
        self.pid, self.master = pty.fork()
        if self.pid == 0:
            os.execv(sys.executable, [sys.executable, str(root / "app.py"), str(root), mode])
        os.set_blocking(self.master, False)
        self.output = bytearray()
        self.status: int | None = None

    def drain(self) -> None:
        try:
            while data := os.read(self.master, 65536):
                self.output.extend(data)
        except BlockingIOError:
            pass
        except OSError as error:
            if error.errno != errno.EIO:
                raise
        if self.status is None:
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.status = os.waitstatus_to_exitcode(status)

    def wait_for(self, path: Path) -> None:
        until = time.monotonic() + 8
        while not path.is_file():
            self.drain()
            if self.status is not None or until < time.monotonic():
                pytest.fail(
                    f"Missing {path.name}, parent exit={self.status}: {self.output[-1500:]!r}"
                )
            time.sleep(0.02)

    def wait_exit(self) -> int:
        until = time.monotonic() + 8
        while self.status is None and time.monotonic() < until:
            self.drain()
            time.sleep(0.02)
        assert self.status is not None
        return self.status

    def send(self, data: bytes) -> None:
        os.write(self.master, data)

    def close(self) -> None:
        self.drain()
        if self.status is None:
            os.killpg(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
        os.close(self.master)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux Ghostty and Textual PTY lifecycle")
def test_legacy_shared_pty_interrupt_exits_parent(terminal: Path) -> None:
    """The old foreground handoff exposes the TUI to the editor's tty signals."""
    app = Terminal(terminal, "legacy")
    try:
        app.wait_for(terminal / "editor-tty")
        editor = json.loads((terminal / "editor-tty").read_text())
        assert editor["tty"] == [(terminal / "parent-tty").read_text()] * 3
        assert editor["group"] == app.pid
        app.send(b"\x03")
        app.wait_exit()
        assert not (terminal / "completed").exists()
    finally:
        app.close()


@pytest.mark.skipif(sys.platform != "linux", reason="Linux Ghostty and Textual PTY lifecycle")
@pytest.mark.parametrize("outcome", ["0", "7", "interrupted", "startup-failure", "no-display"])
def test_editor_quit_preserves_textual_terminal(
    terminal: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    if outcome == "startup-failure":
        monkeypatch.setenv("FORGE_EDITOR_TEST_FAIL", "1")
    if outcome == "no-display":
        monkeypatch.delenv("DISPLAY")
        monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)
    app = Terminal(terminal, "external")
    try:
        if outcome not in {"startup-failure", "no-display"}:
            app.wait_for(terminal / "editor-tty")
            editor = json.loads((terminal / "editor-tty").read_text())
            assert len(set(editor["tty"])) == 1
            assert editor["tty"][0] != (terminal / "parent-tty").read_text()
            assert editor["session"] != app.pid and editor["tmux"] is None
            assert editor["target"] == str(terminal / "target ü.toml")
            assert b"\x1b[?1000h" in app.output
            app.send(b"k")
            app.wait_for(terminal / "k")
            app.send(b"\x1b[<0;2;1M\x1b[<0;2;1m")
            app.wait_for(terminal / "mouse")
            (terminal / "mouse").unlink()
            if outcome == "interrupted":
                os.killpg(editor["group"], signal.SIGINT)
            else:
                (terminal / "exit-status").write_text(outcome)
                (terminal / "release").touch()
        app.wait_for(terminal / "completed")
        assert app.status is None
        assert b"\x1b[?1000l" not in app.output
        app.send(b"j")
        app.wait_for(terminal / "j")
        app.send(b"\x1b[<0;2;1M\x1b[<0;2;1m")
        app.wait_for(terminal / "mouse")
        if outcome == "0":
            assert not (terminal / "error").exists()
        else:
            message = {
                "7": "status 7",
                "interrupted": "without a completion receipt",
                "startup-failure": "before starting the editor",
                "no-display": "No Linux graphical display",
            }[outcome]
            assert message in (terminal / "error").read_text()
        app.send(b"q")
        assert app.wait_exit() == 0
    finally:
        app.close()


@pytest.mark.skipif(sys.platform != "linux", reason="Linux Ghostty and Textual PTY lifecycle")
def test_parent_exit_leaves_editable_window_and_completion_receipt(terminal: Path) -> None:
    app = Terminal(terminal, "external")
    try:
        app.wait_for(terminal / "editor-tty")
        editor = json.loads((terminal / "editor-tty").read_text())
        (receipt,) = terminal.glob(".window-*")
        app.wait_for(receipt / "ready")
        app.send(b"q")
        assert app.wait_exit() == 0
        os.kill(editor["pid"], 0)
        (terminal / "exit-status").write_text("0")
        (terminal / "release").touch()
        until = time.monotonic() + 5
        while not (receipt / "result").is_file() and time.monotonic() < until:
            time.sleep(0.02)
        assert (receipt / "result").read_text() == "0"
    finally:
        (terminal / "exit-status").write_text("0")
        (terminal / "release").touch()
        app.close()
