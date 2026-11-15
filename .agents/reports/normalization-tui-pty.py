"""Exercise the real normalization dashboard at a gated replay phase in a PTY."""
import errno
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import select
import signal
import struct
import subprocess
import termios
import time

root = Path(__file__).resolve().parents[2]
artifacts = root / 'data/tests/historical/normalize/stabilization' / f'pty-{time.time_ns()}'
artifacts.mkdir(parents=True)
os.mkfifo(artifacts / 'release.fifo')
metadata = subprocess.check_output([
    'cargo', 'test', '--release', '--manifest-path', str(root / 'engine/Cargo.toml'),
    '--features', 'historical,monitor-tui,normalization-test-hooks',
    '--test', 'normalization_dashboard', '--no-run', '--message-format=json'], cwd=root)
binaries = [json.loads(line)['executable'] for line in metadata.splitlines()
            if json.loads(line).get('reason') == 'compiler-artifact'
            and json.loads(line).get('executable')
            and json.loads(line)['target']['name'] == 'normalization_dashboard']
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 30, 120, 0, 0))
before = termios.tcgetattr(slave)
env = dict(os.environ, MARKETFORGE_TUI_PTY_DIR=str(artifacts), TERM='xterm-256color')
process = subprocess.Popen([binaries[-1], 'pty_production_loop_harness', '--ignored',
                            '--nocapture', '--test-threads=1'], cwd=root,
                           stdin=slave, stdout=slave, stderr=slave, env=env,
                           start_new_session=True)
output = bytearray()

def screen_text():
    # Minimal CSI cursor/erase interpreter for Ratatui's differential redraws.
    # Looking for a complete timestamp in raw output misses single-digit updates.
    cells = [[' '] * 160 for _ in range(60)]
    row = col = 0
    text = output.decode('utf-8', errors='replace')
    index = 0
    while index < len(text):
        if text[index] == '\x1b':
            match = re.match(r'\x1b\[([?0-9;]*)([@-~])', text[index:])
            if match:
                params, command = match.groups()
                values = [int(v or '0') for v in params.lstrip('?').split(';')]
                if command in 'Hf':
                    row = max(0, (values[0] or 1) - 1)
                    col = max(0, (values[1] if len(values) > 1 else 1) - 1)
                elif command == 'G':
                    col = max(0, (values[0] or 1) - 1)
                elif command == 'J' and values[0] in (2, 3):
                    cells = [[' '] * 160 for _ in range(60)]
                elif command == 'K' and row < 60:
                    cells[row][col:] = [' '] * (160-col)
                index += len(match.group())
                continue
            index += 2
            continue
        char = text[index]
        if char == '\r':
            col = 0
        elif char == '\n':
            row += 1
        elif char >= ' ':
            if row < 60 and col < 160:
                cells[row][col] = char
            col += 1
        index += 1
    return '\n'.join(''.join(line) for line in cells)

def pump(until, timeout=20):
    deadline = time.monotonic() + timeout
    while not until():
        if time.monotonic() > deadline:
            raise AssertionError('PTY condition timed out')
        ready, _, _ = select.select([master], [], [], 0.05)
        if ready:
            try:
                chunk = os.read(master, 65536)
            except OSError as error:
                if error.errno == errno.EIO:
                    break
                raise
            output.extend(chunk)
        if process.poll() is not None and not ready:
            break
    assert until(), bytes(output[-4000:])

try:
    pump(lambda: (artifacts / 'reached').exists())
    os.write(master, b'?')
    pump(lambda: b'Dashboard help' in output)
    # The worker remains blocked at replay; regular redraw still advances time.
    pump(lambda: '00:00:01' in screen_text(), timeout=5)
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 24, 80, 0, 0))
    os.kill(process.pid, signal.SIGWINCH)
    resize_start = len(output)
    os.write(master, b'\x1b[B')
    pump(lambda: len(output) > resize_start)
    os.write(master, b'?')
    with (artifacts / 'release.fifo').open('wb', buffering=0) as gate:
        gate.write(b'1')
    pump(lambda: process.poll() is not None)
    # Drain output, including the guard's cursor/raw-mode restoration.
    while select.select([master], [], [], 0.05)[0]:
        output.extend(os.read(master, 65536))
    assert process.returncode == 0
    assert b'\x1b[?1049h' in output and b'\x1b[?1049l' in output
    assert termios.tcgetattr(slave) == before, 'terminal attributes were not restored'
    assert b'test result: ok' in output
    (artifacts / 'terminal.log').write_bytes(output)
    print(json.dumps({'passed': True, 'artifact_directory': str(artifacts),
                      'help': True, 'quiet_phase_elapsed_redraw': True,
                      'resize': True, 'terminal_restored': True}))
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
    (artifacts / 'terminal.log').write_bytes(output)
    os.close(master)
    os.close(slave)
