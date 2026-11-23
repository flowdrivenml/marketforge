"""Run the real merge Engine and Forge lifecycle against a disposable Linux terminal."""

import errno
import fcntl
import json
import os
import pty
import select
import shutil
import struct
import subprocess
import sys
import termios
import time
from pathlib import Path

mode, binary_arg, job_arg, config_arg = sys.argv[1:]
binary, job, config = map(Path, (binary_arg, job_arg, config_arg))
root = job.parent
result = root / "engine-result.json"


def forge_child():
    import asyncio
    from contextlib import nullcontext
    from unittest.mock import AsyncMock, patch

    from forge.cli.main import main
    from forge.historical.merge import service

    (root / "forge").mkdir()
    (root / "forge/pyproject.toml").touch()
    (root / "engine").mkdir()
    runtime = root / "data/.config/engine/merge.toml"
    runtime.parent.mkdir(parents=True)
    shutil.copyfile(config, runtime)
    document = json.loads(job.read_bytes())
    selected = []
    for entry in document["inputs"]:
        pub = entry["publication"]
        manifest_path = Path(pub["manifest_path"])
        manifest = json.loads(manifest_path.read_bytes())
        primary = Path(entry["path"]).relative_to(manifest_path.parent)
        part = next(item for item in manifest["files"] if item["path"] == str(primary))
        selected.append(
            {
                "database_id": entry["dataset_id"],
                "namespace": "fixture",
                "engine_id": int(pub["engine_dataset_id"]),
                "path": str(manifest_path.parent),
                "primary_file": str(primary),
                "registered_path": entry["path"],
                "manifest_path": str(manifest_path),
                "file_sha256": entry["sha256"],
                "manifest_sha256": pub["manifest_sha256"],
                "start_ns": part["start_timestamp_ns"],
                "end_ns": part["end_timestamp_ns"],
                "stream_id": entry["stream_id"],
                "stream_rank": entry["stream_rank"],
                "content_type": entry["content_type"],
                "memberships": [{"stream_id": entry["stream_id"]}],
            }
        )
    calls = 0

    async def register(_paths, evidence, *, receipt_directory, execution):
        nonlocal calls
        calls += 1
        sole = evidence[0]
        assert execution["registry_id"] == "fixture-registry"
        base = {
            "environment": "testing",
            "publication": "committed",
            "execution": execution,
            "evidence": evidence,
            "registration_receipt": str(receipt_directory / "registration-result.json"),
        }
        if mode == "forge-recover-child" and calls == 1:
            return {
                **base,
                "registration": "failed",
                "error": "Fixture registry unavailable",
            }
        files = {
            str(Path(sole["path"]) / item["path"]): index
            for index, item in enumerate(sole["manifest"]["files"], 42)
        }
        state = "reused" if calls > 2 else "new"
        states = dict.fromkeys(files, state)
        return {
            **base,
            "registration": "registered",
            "catalogue_dataset_ids": files,
            "file_states": states,
            "publications": {
                sole["path"]: {"publication_id": 1, "file_states": states}
            },
            "counts": {
                "publications": 1,
                "primary_files": len(files),
                "new": len(files) if state == "new" else 0,
                "reused": len(files) if state == "reused" else 0,
            },
        }

    def execute(paths, source, *, tui=False):
        return asyncio.run(
            service.execute_async(
                paths,
                source,
                tui=tui,
                standalone=True,
                executable=binary,
                execution_directory=root / "attempt",
            )
        )

    with (
        patch("forge.cli.commands.merge.cli_execution", return_value=nullcontext()),
        patch.object(service, "execute", execute),
        patch.object(service, "resolve", return_value=binary),
        patch.object(
            service, "selected_registry_id", AsyncMock(return_value="fixture-registry")
        ),
        patch.object(service, "select_inputs", AsyncMock(return_value=selected)),
        patch.object(service, "register_verified", register),
    ):
        args = ["--project-root", str(root), "merge"]
        run = [*args, "run", str(job), "--testing"]
        if mode == "forge-prepare-child":
            library = root / "data/.config/workflows/data_engineering/merge"
            library.mkdir(parents=True)
            recipe = library / "fixture.toml"
            recipe.write_text(
                f"dataset_ids = {[entry['database_id'] for entry in selected]}\n"
                f"start_ns = {document['time_range']['start_ns']}\n"
                f"end_ns = {document['time_range']['end_ns']}\n"
            )
            assert main([*args, "list"]) == 0
            assert main([*args, "validate", "fixture"]) == 0
            return main([*args, "prepare", "fixture", "--testing"])
        if mode == "forge-json-child":
            return main(["--json", *run])
        if mode == "forge-recover-child":
            assert main(run) == 1
            recovery = [
                *args,
                "register",
                str(root / "attempt/execution-intent.json"),
                "--testing",
            ]
            assert main(recovery) == 0
            assert main(recovery) == 0 and calls == 3
            assert main([*recovery[:-1], "--production"]) == 2 and calls == 3
            return 0
        return main([*run, "--tui"])


if mode.endswith("-child"):
    sys.exit(forge_child())

managed = mode != "standalone"
cancel = mode.endswith("cancel")
forge = mode.startswith("forge")
tui = mode not in {"forge-json", "forge-recover", "forge-prepare"}
if forge:
    command = [
        sys.executable,
        __file__,
        f"{mode}-child",
        binary_arg,
        job_arg,
        config_arg,
    ]
    result = root / "attempt/engine-result.json"
else:
    command = [
        str(binary),
        "--job",
        str(job),
        "--config",
        str(config),
        "--result-file",
        str(result),
        "--tui",
    ]
    if managed:
        command.extend(["--config-origin", str(config)])

master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 36, 120, 0, 0))
original = termios.tcgetattr(slave)
process = subprocess.Popen(
    command,
    cwd=root,
    env={**os.environ, "TERM": "xterm-256color"},
    stdin=slave,
    stdout=slave,
    stderr=slave,
)
enter, leave = b"\x1b[?1049h", b"\x1b[?1049l"
transcript = bytearray()
sent = False
try:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if select.select([master], [], [], 0.05)[0]:
            try:
                transcript.extend(os.read(master, 65536))
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
        if cancel and not sent and enter in transcript:
            os.write(master, b"\x03")
            sent = True
        if process.poll() is not None and not select.select([master], [], [], 0)[0]:
            break
    expected = (1 if forge else 130) if cancel else 0
    assert process.wait(timeout=2) == expected, bytes(transcript[-4000:])
    assert transcript.count(enter) == transcript.count(leave) == int(tui)
    assert termios.tcgetattr(slave) == original
    after = bytes(transcript.rsplit(leave, 1)[-1])
    if mode == "forge-prepare":
        assert b"ready for submission" in after and b"VALID" in after, after
        assert (root / "data/runs").is_dir()
        print(after.decode())
    elif cancel:
        assert sent
        if forge:
            assert b"INTERRUPTED" in after and after.count(b"MarketForge") == 1, after
        else:
            assert not after.strip(), after
    else:
        envelope = json.loads(result.read_bytes())
        assert envelope["kind"] == "merge_result" and envelope["status"] == "complete"
        if mode == "forge-json":
            retained = json.loads(after)
            assert (
                retained["engine_result"] == envelope
                and retained["status"] == "complete"
            )
        elif mode == "forge-recover":
            assert (
                b"Fixture registry unavailable" in after
                and after.count(b"Published Output") == 3
            ), after
            assert b"submitted_sha256" not in after, after
            print(after.decode())
        elif forge:
            assert after.count(b"MarketForge") == 1 and b"Published Output" in after, (
                after
            )
            assert (
                b"Registry file ID" in after
                and b"REGISTERED"
                not in after.upper().split(b"PHYSICAL VERIFICATION")[0]
            ), after
            assert b'"schema_version"' not in after and b"MERGE SUMMARY" not in after, (
                after
            )
            print(after.decode())
        elif managed:
            assert not after.strip(), after
        else:
            assert b"Manifest" in after and b"Publication" in after, after
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
    os.close(master)
    os.close(slave)
