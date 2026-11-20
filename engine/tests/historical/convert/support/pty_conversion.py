"""Bounded Linux/macOS PTY exercise for the conversion CLI."""
import errno
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import struct
import subprocess
import sys
import termios
import time


def run(args, expected, *, on_screen=None, release=None):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
    before = termios.tcgetattr(slave)
    env = {**os.environ, "TERM": "xterm-256color"}
    env.pop("MARKETFORGE_TUI_HOLD", None)
    process = subprocess.Popen(
        args, stdin=slave, stdout=slave, stderr=slave, start_new_session=True,
        env=env,
    )
    output = bytearray()

    def pump(until):
        deadline = time.monotonic() + 8
        while not until():
            if time.monotonic() >= deadline:
                raise AssertionError(f"PTY timed out: {bytes(output[-4000:])!r}")
            drain(0.05)

    def drain(timeout):
        if select.select([master], [], [], timeout)[0]:
            try:
                output.extend(os.read(master, 65536))
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                return False
            return True
        return False

    try:
        if on_screen is not None:
            pump(lambda: on_screen in output)
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 16, 65, 0, 0))
            os.write(master, b"d\x1b[B\x03")
            pump(lambda: b"INTERRUPT" in output)
            assert process.poll() is None, "conversion returned before the blocked worker was released"
            release()
        pump(lambda: process.poll() is not None)
        while drain(0.05):
            pass
        assert process.returncode == expected, bytes(output[-4000:])
        assert b"\x1b[?1049h" in output and b"\x1b[?1049l" in output
        assert b"\x1b[?25h" in output
        assert termios.tcgetattr(slave) == before, "raw mode was left enabled"
        return bytes(output)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)
        os.close(master)
        os.close(slave)


binary, project, job_path, catalog, config = map(Path, sys.argv[1:])
job = json.loads(job_path.read_text())
output = Path(job["output"]["dataset_path"])
assert output.parent == job_path.parent
base = [str(binary), "--project-root", str(project), "--job", str(job_path), "--config", str(config)]
args = [*base, "--catalog", str(catalog), "--tui"]
screen = run([*args, "--materialize"], 0)
assert b"MATERIALIZED (UNPUBLISHED)" in screen
assert b"\r\nPUBLISHED" not in screen
assert output.with_suffix(".staging").is_dir() and not output.exists()
screen = run([*args, "--publish"], 0)
assert b"PUBLISHED" in screen and b"Manifest " in screen and b"12 rows" in screen
assert (output / "manifest.json").is_file()

job["output"]["dataset_path"] = str(output.parent / "rejected")
job_path.write_text(json.dumps(job))
screen = run([*base, "--catalog", str(catalog.with_name("missing.json")), "--tui"], 1)
assert b"FAILED" in screen and b"missing.json" in screen
assert not Path(job["output"]["dataset_path"]).exists()

fifo = output.parent / "catalog.fifo"
os.mkfifo(fifo)
gate = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)


def release_catalog():
    global gate
    os.write(gate, catalog.read_bytes())
    os.close(gate)
    gate = None


try:
    screen = run(
        [*base, "--catalog", str(fifo), "--tui"], 130,
        on_screen=b"CATALOG RESOLUTION", release=release_catalog,
    )
    assert b"Conversion interrupted" in screen
    assert not Path(job["output"]["dataset_path"]).with_suffix(".staging").exists()
finally:
    if gate is not None:
        os.close(gate)

# Terminal setup failure must abort before any conversion writes.
failed_setup = subprocess.run(
    [*args, "--run"], stdin=subprocess.DEVNULL,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5,
)
assert failed_setup.returncode == 1 and b"dashboard error" in failed_setup.stderr
assert not Path(job["output"]["dataset_path"]).with_suffix(".staging").exists()
