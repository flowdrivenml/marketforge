"""Drive the real normalization dashboard on a Linux controlling terminal."""

import errno
import fcntl
import json
import os
import pty
import select
import signal
import struct
import sys
import termios
import time
from pathlib import Path


def forge_child(command, *, machine):
    """Use the real Forge command, supervisor, verification and renderer with a fixture registry."""
    import asyncio
    import shutil
    from contextlib import nullcontext
    from unittest.mock import AsyncMock, patch

    from forge.cli.main import main
    from forge.historical.normalization import execution

    root = Path(command[command.index("--project-root") + 1])
    batch = Path(command[command.index("--batch") + 1])
    directory = batch.parent
    workspace = directory / "forge-workspace"
    (workspace / "forge").mkdir(parents=True)
    (workspace / "forge/pyproject.toml").touch()
    (workspace / "engine").mkdir()

    async def register(_paths, verified, **_kwargs):
        publication = verified[0]
        primary = str(
            Path(publication["path"]) / publication["manifest"]["files"][0]["path"]
        )
        return {
            "registration": "registered",
            "registration_receipt": str(directory / "registration-result.json"),
            "catalogue_dataset_ids": {primary: 42},
            "publications": {publication["path"]: {"publication_id": 77}},
            "file_states": {primary: "new"},
            "counts": {"publications": 1, "primary_files": 1, "new": 1, "reused": 0},
        }

    historical = directory / "fixture-historical"
    historical.mkdir()
    runtime = Path(command[command.index("--config") + 1])
    shutil.copyfile(runtime, historical / "normalize.toml")
    shutil.copytree(
        root / "data/.config/engine/normalize/profiles", historical / "profiles"
    )

    def run(paths, source, *, tui, allow_partial):
        document = json.loads(source.read_bytes())
        raw = {str(job["job_id"]): {} for job in document["jobs"]}
        with (
            patch.object(
                execution,
                "selected_registry_id",
                AsyncMock(return_value="registry-fixture"),
            ),
            patch.object(execution, "pin_raw_inputs", AsyncMock(return_value=raw)),
            patch.object(execution, "register_verified", register),
        ):
            return asyncio.run(
                execution.execute_async(
                    paths,
                    source,
                    tui=tui,
                    allow_partial=allow_partial,
                    standalone=True,
                    directory=directory / "forge-execution",
                    historical_root=historical,
                    executable=Path(command[0]),
                )
            )

    with (
        patch(
            "forge.cli.commands.normalization.cli_execution", return_value=nullcontext()
        ),
        patch("forge.cli.commands.normalization.execute", run),
    ):
        args = [
            "--project-root",
            str(workspace),
            "normalize",
            "run",
            str(batch),
            "--testing",
        ]
        return main(["--json", *args] if machine else [*args, "--tui"])


mode, *command = sys.argv[1:]
if mode in {
    "forge-child",
    "forge-json-child",
    "forge-cancel-child",
    "forge-signal-child",
}:
    sys.exit(forge_child(command, machine=mode == "forge-json-child"))
result = Path(command[command.index("--result-file") + 1])
if mode in {"managed", "cancel"}:
    command.append("--managed-output")
if mode.startswith("forge"):
    result = result.parent / "forge-execution" / result.name
    command = [sys.executable, __file__, f"{mode}-child", *command]


# Keep an observer fd open so terminal attributes can be compared after exit.
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 36, 120, 0, 0))
original = termios.tcgetattr(slave)
pid = os.fork()
if pid == 0:
    os.setsid()
    fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
    os.close(master)
    for fd in (0, 1, 2):
        os.dup2(slave, fd)
    os.close(slave)
    os.environ["TERM"] = "xterm-256color"
    os.execv(command[0], command)

enter = b"\x1b[?1049h"
leave = b"\x1b[?1049l"
transcript = bytearray()
deadline = time.monotonic() + 15
sent = False
status = None
try:
    while time.monotonic() < deadline:
        readable, _, _ = select.select([master], [], [], 0.05)
        if readable:
            transcript.extend(os.read(master, 65536))
        if not sent and enter in transcript and mode.endswith("cancel"):
            os.write(master, b"?\x1b\x03")
            sent = True
        if not sent and enter in transcript and mode == "forge-signal":
            children = Path(f"/proc/{pid}/task/{pid}/children").read_text().split()
            assert len(children) == 1
            os.kill(int(children[0]), signal.SIGTERM)
            sent = True
        child, code = os.waitpid(pid, os.WNOHANG)
        if child:
            status = os.waitstatus_to_exitcode(code)
            while select.select([master], [], [], 0)[0]:
                transcript.extend(os.read(master, 65536))
            break
    assert status == (130 if mode.endswith(("cancel", "signal")) else 0), (
        status,
        transcript[-4000:],
    )
    screen_switches = int(mode != "forge-json")
    assert transcript.count(enter) == transcript.count(leave) == screen_switches
    assert termios.tcgetattr(slave) == original
    final_screen = bytes(transcript.rsplit(leave, 1)[-1])
    after = final_screen.strip()
    if mode == "forge-signal":
        assert sent and not result.exists()
        receipt = json.loads(result.with_name("execution-result.json").read_bytes())
        assert receipt["normalization"] == "interrupted"
        assert receipt["exit_code"] == -signal.SIGTERM
    else:
        receipt = json.loads(result.read_bytes())
        assert receipt["kind"] == "normalization_result"
    if mode == "standalone":
        assert b"PROCESSING SESSION COMPLETE" in after
        assert b"Normalization diagnostics:" in after
        assert b"Published manifest:" in after
        assert json.loads(after.splitlines()[-1]) == receipt
    elif mode == "forge-json":
        rendered = json.loads(after)
        assert rendered["engine_result"] == receipt
        assert rendered["status"] == "complete"
        assert all(
            Path(path).is_absolute() for path in rendered["catalogue_dataset_ids"]
        )
        assert b"\x1b" not in after
    elif mode in {"forge", "forge-cancel", "forge-signal"}:
        assert final_screen.startswith(b"\r\n") and not final_screen.startswith(
            b"\r\n\r\n"
        )
        assert final_screen.endswith(b"\r\n\r\n") and not final_screen.endswith(
            b"\r\n\r\n\r\n"
        )
        assert after.count(b"MarketForge") == 1, after
        if mode == "forge":
            assert b"Published Datasets" in after and b"9007199254740995" in after
            assert b"COMPLETED" in after and b"REGISTERED" in after
        else:
            assert b"INTERRUPTED" in after
            if mode == "forge-cancel":
                assert b"normalization cancelled" in after
            assert b"engine-tui.log" in after and b"mf normalize register" in after
        assert b"PROCESSING SESSION COMPLETE" not in after
        assert b"Published manifest:" not in after and b'"schema_version"' not in after
        assert result.with_name("execution-result.json").is_file()
        assert result.with_name("engine-tui.log").is_file()
    else:
        assert not after, after
        assert result.with_name("engine-tui.log").is_file()
    if mode.endswith("cancel"):
        assert sent and receipt["status"] == "failed"
        assert receipt["error"]["message"] == "normalization cancelled"
finally:
    if status is None:
        try:
            os.kill(pid, 9)
        except OSError as error:
            if error.errno != errno.ESRCH:
                raise
        os.waitpid(pid, 0)
    os.close(master)
    os.close(slave)
