#!/usr/bin/env python3
"""Atomic task coordination for StillFlow WSL agents.

The machine source of truth lives on the never-merged
``coordination/task-registry`` branch. Registry mutations create one Git commit
containing both ``registry.json`` and the rendered ``TASKS.md``. Moving the
branch ref is non-forced, so a concurrent mutation based on an older head is
rejected instead of overwriting another agent's claim.

Only the Python standard library and an authenticated ``gh`` CLI are required.
"""

from __future__ import annotations

import argparse
import base64
import copy
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import quote


DEFAULT_REPOSITORY = "X44421/stillflow"
DEFAULT_COORDINATION_BRANCH = "coordination/task-registry"
REGISTRY_PATH = "coordination/registry.json"
TASKS_PATH = "coordination/TASKS.md"

ALLOWED_STATUSES = {
    "blocked",
    "cancelled_duplicate",
    "dispatched",
    "done",
    "failed",
    "hold",
    "queued",
    "running",
    "wait_maintainer",
}
ALLOWED_MODES = {"maintenance", "review", "write", "measurement", "design"}
TERMINAL_STATUSES = {"cancelled_duplicate", "done"}
CLAIMABLE_STATUSES = {"dispatched", "queued"}


class RegistryError(RuntimeError):
    """A registry invariant or requested transition is invalid."""


class RemoteConflict(RuntimeError):
    """The coordination branch changed during a compare-and-swap mutation."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def task_map(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {task["id"]: task for task in registry["tasks"]}


def get_task(registry: dict[str, Any], task_id: str) -> dict[str, Any]:
    for task in registry["tasks"]:
        if task["id"] == task_id:
            return task
    raise RegistryError(f"unknown task: {task_id}")


def require_agent(value: str | None) -> str:
    agent = value or os.environ.get("STILLFLOW_AGENT_ID")
    if not agent:
        raise RegistryError(
            "agent id is required: pass --agent or set STILLFLOW_AGENT_ID"
        )
    return agent


def validate_registry(registry: dict[str, Any]) -> None:
    if registry.get("schema_version") != 1:
        raise RegistryError("unsupported registry schema_version")
    if not isinstance(registry.get("revision"), int) or registry["revision"] < 0:
        raise RegistryError("revision must be a non-negative integer")
    if not isinstance(registry.get("tasks"), list):
        raise RegistryError("tasks must be an array")
    if not isinstance(registry.get("locks"), dict):
        raise RegistryError("locks must be an object")

    ids: set[str] = set()
    tasks = task_map(registry)
    if len(tasks) != len(registry["tasks"]):
        raise RegistryError("task ids must be unique")

    for task in registry["tasks"]:
        task_id = task.get("id")
        if not isinstance(task_id, str) or not task_id:
            raise RegistryError("every task needs a non-empty string id")
        if task_id in ids:
            raise RegistryError(f"duplicate task id: {task_id}")
        ids.add(task_id)
        if task.get("status") not in ALLOWED_STATUSES:
            raise RegistryError(f"{task_id}: invalid status {task.get('status')!r}")
        if task.get("mode") not in ALLOWED_MODES:
            raise RegistryError(f"{task_id}: invalid mode {task.get('mode')!r}")
        if not isinstance(task.get("locks", []), list):
            raise RegistryError(f"{task_id}: locks must be an array")
        if not isinstance(task.get("depends_on", []), list):
            raise RegistryError(f"{task_id}: depends_on must be an array")
        if not isinstance(task.get("conflicts_with", []), list):
            raise RegistryError(f"{task_id}: conflicts_with must be an array")
        for dependency in task.get("depends_on", []):
            if dependency not in tasks:
                raise RegistryError(f"{task_id}: unknown dependency {dependency}")
        for conflict in task.get("conflicts_with", []):
            if conflict not in tasks:
                raise RegistryError(f"{task_id}: unknown conflicting task {conflict}")
            if conflict == task_id:
                raise RegistryError(f"{task_id}: task cannot conflict with itself")
        expected_head = task.get("expected_head")
        if expected_head and not re.fullmatch(r"[0-9a-f]{40}", expected_head):
            raise RegistryError(f"{task_id}: expected_head must be a full lowercase SHA")
        result_branch = task.get("result_branch")
        if result_branch is not None and (
            not isinstance(result_branch, str) or not result_branch
        ):
            raise RegistryError(f"{task_id}: result_branch must be a non-empty string")
        if task["status"] == "running":
            if not task.get("owner"):
                raise RegistryError(f"{task_id}: running task needs an owner")
            if not task.get("lease_expires_at"):
                raise RegistryError(f"{task_id}: running task needs a lease")
            for lock_key in task.get("locks", []):
                lock = registry["locks"].get(lock_key)
                if not lock or lock.get("task_id") != task_id:
                    raise RegistryError(
                        f"{task_id}: running task does not own requested lock {lock_key}"
                    )

    for lock_key, lock in registry["locks"].items():
        if not isinstance(lock_key, str) or not lock_key:
            raise RegistryError("lock keys must be non-empty strings")
        task_id = lock.get("task_id")
        if task_id not in tasks:
            raise RegistryError(f"{lock_key}: unknown lock task {task_id}")
        task = tasks[task_id]
        if task["status"] != "running":
            raise RegistryError(f"{lock_key}: owner task {task_id} is not running")
        if lock.get("owner") != task.get("owner"):
            raise RegistryError(f"{lock_key}: owner differs from task {task_id}")
        if lock_key not in task.get("locks", []):
            raise RegistryError(f"{lock_key}: absent from task {task_id} lock request")


def dependency_blockers(registry: dict[str, Any], task: dict[str, Any]) -> list[str]:
    tasks = task_map(registry)
    return [
        dependency
        for dependency in task.get("depends_on", [])
        if tasks[dependency]["status"] not in TERMINAL_STATUSES
    ]


def active_lock_conflicts(
    registry: dict[str, Any], task: dict[str, Any], now: datetime
) -> list[str]:
    conflicts: list[str] = []
    tasks = task_map(registry)
    for other_id, other in tasks.items():
        if other_id == task["id"] or other["status"] != "running":
            continue
        if (
            other_id in task.get("conflicts_with", [])
            or task["id"] in other.get("conflicts_with", [])
        ):
            conflicts.append(
                f"task conflict with {other_id} / {other.get('owner')} (running)"
            )
    for lock_key in task.get("locks", []):
        held = registry["locks"].get(lock_key)
        if not held or held.get("task_id") == task["id"]:
            continue
        lease = parse_iso(held.get("lease_expires_at"))
        state = "stale" if lease and lease <= now else "active"
        conflicts.append(
            f"{lock_key} held by {held.get('task_id')} / {held.get('owner')} ({state})"
        )
    return conflicts


def release_task_locks(registry: dict[str, Any], task_id: str) -> None:
    registry["locks"] = {
        key: lock
        for key, lock in registry["locks"].items()
        if lock.get("task_id") != task_id
    }


def claim_in_memory(
    registry: dict[str, Any], task_id: str, agent: str, lease_minutes: int
) -> None:
    task = get_task(registry, task_id)
    if task["status"] not in CLAIMABLE_STATUSES:
        raise RegistryError(
            f"{task_id} is {task['status']}; only queued/dispatched tasks may be claimed"
        )
    blockers = dependency_blockers(registry, task)
    if blockers:
        raise RegistryError(f"{task_id} dependencies are not terminal: {', '.join(blockers)}")
    now = utc_now()
    conflicts = active_lock_conflicts(registry, task, now)
    if conflicts:
        raise RegistryError("lock conflict: " + "; ".join(conflicts))
    if lease_minutes <= 0:
        raise RegistryError("lease minutes must be positive")
    lease = iso(now + timedelta(minutes=lease_minutes))
    task.update(
        {
            "status": "running",
            "owner": agent,
            "lease_expires_at": lease,
            "started_at": task.get("started_at") or iso(now),
            "updated_at": iso(now),
        }
    )
    for lock_key in task.get("locks", []):
        registry["locks"][lock_key] = {
            "task_id": task_id,
            "owner": agent,
            "expected_head": task.get("expected_head"),
            "acquired_at": iso(now),
            "lease_expires_at": lease,
        }


def complete_in_memory(
    registry: dict[str, Any],
    task_id: str,
    agent: str,
    result_head: str | None,
    ci_url: str | None,
    note: str | None,
) -> None:
    task = get_task(registry, task_id)
    if task["status"] != "running" or task.get("owner") != agent:
        raise RegistryError(f"{task_id} is not running under owner {agent}")
    if (task["mode"] == "write" or task.get("result_branch")) and not result_head:
        raise RegistryError("this task requires --result-head on completion")
    if result_head and not re.fullmatch(r"[0-9a-f]{40}", result_head):
        raise RegistryError("--result-head must be a full lowercase SHA")
    now = iso(utc_now())
    release_task_locks(registry, task_id)
    task.update(
        {
            "status": "done",
            "owner": agent,
            "lease_expires_at": null_value(),
            "completed_at": now,
            "updated_at": now,
            "result_head": result_head,
            "ci_url": ci_url,
        }
    )
    if note:
        task["completion_note"] = note


def null_value() -> None:
    return None


def render_markdown(registry: dict[str, Any]) -> str:
    rows = [
        "# StillFlow coordinated task registry",
        "",
        "> Machine source: `coordination/registry.json`. Do not edit this file directly.",
        "",
        f"- Registry revision: `{registry['revision']}`",
        f"- Updated: `{registry['updated_at']}`",
        f"- Source main: `{registry['source_main_sha']}`",
        "",
        "## Tasks",
        "",
        "| ID | Status | Mode | Owner | Target | Expected head | Dependencies | Locks |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for task in registry["tasks"]:
        target = (
            f"PR #{task['target_pr']}"
            if task.get("target_pr") is not None
            else task.get("target_branch") or "—"
        )
        owner = task.get("owner") or "—"
        expected = task.get("expected_head") or "—"
        dependencies = ", ".join(task.get("depends_on", [])) or "—"
        locks = ", ".join(task.get("locks", [])) or "—"
        rows.append(
            f"| `{task['id']}` | **{task['status']}** | {task['mode']} | "
            f"`{owner}` | {target} | `{expected}` | {dependencies} | {locks} |"
        )

    rows.extend(
        [
            "",
            "## Active locks",
            "",
            "| Lock | Task | Owner | Expected head | Lease expires |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    if registry["locks"]:
        for key, lock in sorted(registry["locks"].items()):
            rows.append(
                f"| `{key}` | `{lock['task_id']}` | `{lock['owner']}` | "
                f"`{lock.get('expected_head') or '—'}` | `{lock['lease_expires_at']}` |"
            )
    else:
        rows.append("| — | — | — | — | — |")

    rows.extend(
        [
            "",
            "## WSL agent protocol",
            "",
            "```bash",
            "export STILLFLOW_AGENT_ID=wsl-agent-01",
            "python3 coordination/taskctl.py doctor",
            "python3 coordination/taskctl.py show",
            "python3 coordination/taskctl.py claim TASK_ID",
            "python3 coordination/taskctl.py heartbeat TASK_ID",
            "# Revalidate the target branch head before every code push.",
            "python3 coordination/taskctl.py complete TASK_ID --result-head FULL_SHA --ci URL",
            "```",
            "",
            "A mutation rejected as a remote conflict must be re-read and evaluated; never blind-retry.",
            "Feature branches must never merge this coordination branch.",
            "",
        ]
    )
    return "\n".join(rows)


def gh_json(method: str, endpoint: str, payload: dict[str, Any] | None = None) -> Any:
    command = ["gh", "api", "--method", method, endpoint]
    input_data: str | None = None
    if payload is not None:
        command.extend(["--input", "-"])
        input_data = json.dumps(payload, separators=(",", ":"))
    result = subprocess.run(
        command,
        input=input_data,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "gh api failed"
        raise RuntimeError(message)
    if not result.stdout.strip():
        return None
    return json.loads(result.stdout)


class RemoteRegistry:
    def __init__(self, repository: str, branch: str) -> None:
        self.repository = repository
        self.branch = branch

    def branch_head(self) -> str:
        ref = quote(f"heads/{self.branch}", safe="/")
        payload = gh_json("GET", f"repos/{self.repository}/git/ref/{ref}")
        return payload["object"]["sha"]

    def target_branch_head(self, branch: str) -> str:
        payload = gh_json(
            "GET", f"repos/{self.repository}/branches/{quote(branch, safe='')}"
        )
        return payload["commit"]["sha"]

    def read_file(self, path: str) -> str:
        endpoint = (
            f"repos/{self.repository}/contents/{quote(path, safe='/')}"
            f"?ref={quote(self.branch, safe='')}"
        )
        payload = gh_json("GET", endpoint)
        return base64.b64decode(payload["content"]).decode("utf-8")

    def read_registry(self) -> dict[str, Any]:
        registry = json.loads(self.read_file(REGISTRY_PATH))
        validate_registry(registry)
        return registry

    def atomic_write(
        self, expected_head: str, registry: dict[str, Any], message: str
    ) -> str:
        validate_registry(registry)
        registry_text = json.dumps(registry, ensure_ascii=False, indent=2) + "\n"
        tasks_text = render_markdown(registry)

        commit = gh_json(
            "GET", f"repos/{self.repository}/git/commits/{expected_head}"
        )
        base_tree = commit["tree"]["sha"]
        registry_blob = gh_json(
            "POST",
            f"repos/{self.repository}/git/blobs",
            {"content": registry_text, "encoding": "utf-8"},
        )["sha"]
        tasks_blob = gh_json(
            "POST",
            f"repos/{self.repository}/git/blobs",
            {"content": tasks_text, "encoding": "utf-8"},
        )["sha"]
        tree = gh_json(
            "POST",
            f"repos/{self.repository}/git/trees",
            {
                "base_tree": base_tree,
                "tree": [
                    {
                        "path": REGISTRY_PATH,
                        "mode": "100644",
                        "type": "blob",
                        "sha": registry_blob,
                    },
                    {
                        "path": TASKS_PATH,
                        "mode": "100644",
                        "type": "blob",
                        "sha": tasks_blob,
                    },
                ],
            },
        )["sha"]
        new_commit = gh_json(
            "POST",
            f"repos/{self.repository}/git/commits",
            {"message": message, "tree": tree, "parents": [expected_head]},
        )["sha"]
        ref = quote(f"heads/{self.branch}", safe="/")
        try:
            gh_json(
                "PATCH",
                f"repos/{self.repository}/git/refs/{ref}",
                {"sha": new_commit, "force": False},
            )
        except RuntimeError as error:
            raise RemoteConflict(
                "coordination state changed or ref update was rejected; re-run show and "
                "re-evaluate the task before retrying"
            ) from error
        return new_commit

    def mutate(
        self,
        message: str,
        mutator: Callable[[dict[str, Any]], None],
    ) -> tuple[str, dict[str, Any]]:
        expected_head = self.branch_head()
        registry = self.read_registry()
        mutator(registry)
        registry["revision"] += 1
        registry["updated_at"] = iso(utc_now())
        validate_registry(registry)
        new_head = self.atomic_write(expected_head, registry, message)
        return new_head, registry


def verify_bound_head(remote: RemoteRegistry, task: dict[str, Any]) -> None:
    branch = task.get("target_branch")
    expected = task.get("expected_head")
    if not branch or not expected:
        return
    actual = remote.target_branch_head(branch)
    if actual != expected:
        raise RegistryError(
            f"{task['id']} head mismatch: expected {expected}, remote {actual}"
        )


def command_show(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    registry = remote.read_registry()
    if args.json:
        print(json.dumps(registry, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(registry))


def command_sync(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    registry = remote.read_registry()
    output = Path(args.directory).resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "registry.json").write_text(
        json.dumps(registry, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (output / "TASKS.md").write_text(render_markdown(registry), encoding="utf-8")
    print(f"synced revision {registry['revision']} to {output}")


def command_claim(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)
    snapshot = remote.read_registry()
    verify_bound_head(remote, get_task(snapshot, args.task_id))

    def mutate(registry: dict[str, Any]) -> None:
        claim_in_memory(registry, args.task_id, agent, args.lease_minutes)

    head, _ = remote.mutate(f"coord: claim {args.task_id} by {agent}", mutate)
    print(f"claimed {args.task_id}; coordination head {head}")


def command_adopt(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)
    if args.lease_minutes <= 0:
        raise RegistryError("lease minutes must be positive")

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] != "running":
            raise RegistryError(f"{args.task_id} is not running")
        old_owner = task.get("owner") or ""
        if not old_owner.startswith("external:"):
            raise RegistryError(
                f"{args.task_id} owner {old_owner!r} is not a transferable placeholder"
            )
        now = utc_now()
        lease = iso(now + timedelta(minutes=args.lease_minutes))
        task.update({"owner": agent, "lease_expires_at": lease, "updated_at": iso(now)})
        for lock in registry["locks"].values():
            if lock.get("task_id") == args.task_id:
                lock["owner"] = agent
                lock["lease_expires_at"] = lease

    head, _ = remote.mutate(f"coord: adopt {args.task_id} by {agent}", mutate)
    print(f"adopted {args.task_id}; coordination head {head}")


def command_heartbeat(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)
    if args.lease_minutes <= 0:
        raise RegistryError("lease minutes must be positive")

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] != "running" or task.get("owner") != agent:
            raise RegistryError(f"{args.task_id} is not running under owner {agent}")
        now = utc_now()
        lease = iso(now + timedelta(minutes=args.lease_minutes))
        task.update({"lease_expires_at": lease, "updated_at": iso(now)})
        for lock in registry["locks"].values():
            if lock.get("task_id") == args.task_id:
                lock["lease_expires_at"] = lease

    head, _ = remote.mutate(f"coord: heartbeat {args.task_id} by {agent}", mutate)
    print(f"renewed {args.task_id}; coordination head {head}")


def command_pause(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] != "running" or task.get("owner") != agent:
            raise RegistryError(f"{args.task_id} is not running under owner {agent}")
        release_task_locks(registry, args.task_id)
        now = iso(utc_now())
        task.update(
            {
                "status": "queued",
                "owner": None,
                "lease_expires_at": None,
                "updated_at": now,
                "pause_reason": args.reason,
            }
        )

    head, _ = remote.mutate(f"coord: pause {args.task_id} by {agent}", mutate)
    print(f"paused {args.task_id}; coordination head {head}")


def command_complete(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)
    snapshot = remote.read_registry()
    task = get_task(snapshot, args.task_id)
    effective_result_head = args.result_head
    if task["mode"] == "review":
        verify_bound_head(remote, task)
        effective_result_head = effective_result_head or task.get("expected_head")
    completion_branch = task.get("result_branch")
    if not completion_branch and task["mode"] == "write":
        completion_branch = task.get("target_branch")
    if completion_branch:
        if not effective_result_head:
            raise RegistryError("this task requires --result-head on completion")
        actual = remote.target_branch_head(completion_branch)
        if actual != effective_result_head:
            raise RegistryError(
                f"{args.task_id} completion head mismatch: "
                f"reported {effective_result_head}, remote {actual}"
            )

    def mutate(registry: dict[str, Any]) -> None:
        complete_in_memory(
            registry,
            args.task_id,
            agent,
            effective_result_head,
            args.ci,
            args.note,
        )

    head, _ = remote.mutate(f"coord: complete {args.task_id} by {agent}", mutate)
    print(f"completed {args.task_id}; coordination head {head}")


def command_cancel_duplicate(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] == "running" and task.get("owner") != agent:
            raise RegistryError(f"{args.task_id} is owned by {task.get('owner')}")
        release_task_locks(registry, args.task_id)
        now = iso(utc_now())
        task.update(
            {
                "status": "cancelled_duplicate",
                "owner": agent,
                "lease_expires_at": None,
                "updated_at": now,
                "completed_at": now,
                "covered_by": args.covered_by,
                "completion_note": args.reason,
            }
        )

    head, _ = remote.mutate(
        f"coord: cancel duplicate {args.task_id} covered by {args.covered_by}", mutate
    )
    print(f"cancelled duplicate {args.task_id}; coordination head {head}")


def command_reclaim(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] != "running":
            raise RegistryError(f"{args.task_id} is not running")
        lease = parse_iso(task.get("lease_expires_at"))
        if not lease or lease > utc_now():
            raise RegistryError(f"{args.task_id} lease is not expired")
        release_task_locks(registry, args.task_id)
        task.update(
            {
                "status": "queued",
                "owner": None,
                "lease_expires_at": None,
                "updated_at": iso(utc_now()),
                "reclaim_reason": args.reason,
                "reclaimed_by": agent,
            }
        )

    head, _ = remote.mutate(f"coord: reclaim stale {args.task_id} by {agent}", mutate)
    print(f"reclaimed stale {args.task_id}; coordination head {head}")


def command_queue(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] in {"running", "done", "cancelled_duplicate"}:
            raise RegistryError(f"cannot queue {args.task_id} from {task['status']}")
        blockers = dependency_blockers(registry, task)
        if blockers:
            raise RegistryError(
                f"{args.task_id} dependencies are not terminal: {', '.join(blockers)}"
            )
        task.update(
            {
                "status": "queued",
                "updated_at": iso(utc_now()),
                "queued_by": agent,
                "queue_reason": args.reason,
            }
        )

    head, _ = remote.mutate(f"coord: queue {args.task_id} by {agent}", mutate)
    print(f"queued {args.task_id}; coordination head {head}")


def command_bind_head(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    agent = require_agent(args.agent)

    def mutate(registry: dict[str, Any]) -> None:
        task = get_task(registry, args.task_id)
        if task["status"] == "running":
            raise RegistryError("cannot rebind a running task")
        if args.branch:
            task["target_branch"] = args.branch
        task.update(
            {
                "expected_head": args.head,
                "updated_at": iso(utc_now()),
                "head_bound_by": agent,
                "head_bind_reason": args.reason,
            }
        )

    head, _ = remote.mutate(f"coord: bind {args.task_id} to {args.head}", mutate)
    print(f"bound {args.task_id}; coordination head {head}")


def command_verify(remote: RemoteRegistry, args: argparse.Namespace) -> None:
    registry = remote.read_registry()
    failures: list[str] = []
    for task in registry["tasks"]:
        if args.task_id and task["id"] != args.task_id:
            continue
        try:
            verify_bound_head(remote, task)
        except (RegistryError, RuntimeError) as error:
            failures.append(str(error))
    if failures:
        raise RegistryError("; ".join(failures))
    print("all selected head bindings match")


def command_doctor(remote: RemoteRegistry, _args: argparse.Namespace) -> None:
    if shutil.which("gh") is None:
        raise RegistryError("gh CLI is not installed")
    gh_json("GET", "user")
    registry = remote.read_registry()
    head = remote.branch_head()
    print(
        f"authenticated; repository={remote.repository}; branch={remote.branch}; "
        f"coordination_head={head}; revision={registry['revision']}"
    )


def command_validate_local(_remote: RemoteRegistry, args: argparse.Namespace) -> None:
    registry = json.loads(Path(args.file).read_text(encoding="utf-8"))
    validate_registry(registry)
    print(f"valid registry schema; revision={registry['revision']}")


def command_render_local(_remote: RemoteRegistry, args: argparse.Namespace) -> None:
    registry = json.loads(Path(args.file).read_text(encoding="utf-8"))
    validate_registry(registry)
    rendered = render_markdown(registry)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
        print(args.output)
    else:
        print(rendered)


def command_self_test(_remote: RemoteRegistry, _args: argparse.Namespace) -> None:
    now = iso(utc_now())
    registry = {
        "schema_version": 1,
        "revision": 0,
        "repository": DEFAULT_REPOSITORY,
        "coordination_branch": DEFAULT_COORDINATION_BRANCH,
        "source_main_sha": "0" * 40,
        "updated_at": now,
        "policy": {},
        "tasks": [
            {
                "id": "A",
                "title": "first",
                "status": "queued",
                "mode": "write",
                "owner": None,
                "locks": ["branch:test"],
                "depends_on": [],
                "lease_expires_at": None,
            },
            {
                "id": "B",
                "title": "second",
                "status": "queued",
                "mode": "write",
                "owner": None,
                "locks": ["branch:test"],
                "depends_on": [],
                "lease_expires_at": None,
            },
            {
                "id": "C",
                "title": "merge-like maintenance",
                "status": "queued",
                "mode": "maintenance",
                "owner": None,
                "result_branch": "main",
                "locks": [],
                "depends_on": [],
                "lease_expires_at": None,
            },
            {
                "id": "D",
                "title": "explicit task conflict",
                "status": "queued",
                "mode": "maintenance",
                "owner": None,
                "locks": [],
                "depends_on": [],
                "conflicts_with": ["C"],
                "lease_expires_at": None,
            },
        ],
        "locks": {},
    }
    validate_registry(registry)
    claim_in_memory(registry, "A", "agent-a", 90)
    try:
        claim_in_memory(registry, "B", "agent-b", 90)
    except RegistryError:
        pass
    else:
        raise RegistryError("self-test failed: conflicting claim was accepted")
    complete_in_memory(registry, "A", "agent-a", "1" * 40, None, None)
    claim_in_memory(registry, "B", "agent-b", 90)
    claim_in_memory(registry, "C", "agent-c", 90)
    try:
        claim_in_memory(registry, "D", "agent-d", 90)
    except RegistryError:
        pass
    else:
        raise RegistryError("self-test failed: explicit task conflict was accepted")
    try:
        complete_in_memory(registry, "C", "agent-c", None, None, None)
    except RegistryError:
        pass
    else:
        raise RegistryError("self-test failed: merge result head was not required")
    complete_in_memory(registry, "C", "agent-c", "2" * 40, None, None)
    claim_in_memory(registry, "D", "agent-d", 90)
    validate_registry(registry)
    print(
        "self-test passed: lock/task conflicts rejected, releases observed, "
        "merge head required"
    )


def add_agent_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--agent", help="agent id; defaults to STILLFLOW_AGENT_ID"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo",
        default=os.environ.get("STILLFLOW_REPO", DEFAULT_REPOSITORY),
        help="GitHub repository in owner/name form",
    )
    parser.add_argument(
        "--coord-branch",
        default=os.environ.get(
            "STILLFLOW_COORDINATION_BRANCH", DEFAULT_COORDINATION_BRANCH
        ),
        help="never-merged coordination branch",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    show = subparsers.add_parser("show", help="show current remote registry")
    show.add_argument("--json", action="store_true")
    show.set_defaults(handler=command_show)

    sync = subparsers.add_parser("sync", help="write a local read-only cache")
    sync.add_argument("directory")
    sync.set_defaults(handler=command_sync)

    claim = subparsers.add_parser("claim", help="atomically claim a task")
    claim.add_argument("task_id")
    add_agent_argument(claim)
    claim.add_argument("--lease-minutes", type=int, default=90)
    claim.set_defaults(handler=command_claim)

    adopt = subparsers.add_parser(
        "adopt", help="replace an external placeholder owner on a running task"
    )
    adopt.add_argument("task_id")
    add_agent_argument(adopt)
    adopt.add_argument("--lease-minutes", type=int, default=90)
    adopt.set_defaults(handler=command_adopt)

    heartbeat = subparsers.add_parser("heartbeat", help="renew a task lease")
    heartbeat.add_argument("task_id")
    add_agent_argument(heartbeat)
    heartbeat.add_argument("--lease-minutes", type=int, default=90)
    heartbeat.set_defaults(handler=command_heartbeat)

    pause = subparsers.add_parser("pause", help="release locks and requeue a task")
    pause.add_argument("task_id")
    add_agent_argument(pause)
    pause.add_argument("--reason", required=True)
    pause.set_defaults(handler=command_pause)

    complete = subparsers.add_parser("complete", help="complete and release a task")
    complete.add_argument("task_id")
    add_agent_argument(complete)
    complete.add_argument("--result-head")
    complete.add_argument("--ci")
    complete.add_argument("--note")
    complete.set_defaults(handler=command_complete)

    duplicate = subparsers.add_parser(
        "cancel-duplicate", help="cancel a task covered by a completed task"
    )
    duplicate.add_argument("task_id")
    add_agent_argument(duplicate)
    duplicate.add_argument("--covered-by", required=True)
    duplicate.add_argument("--reason", required=True)
    duplicate.set_defaults(handler=command_cancel_duplicate)

    reclaim = subparsers.add_parser("reclaim", help="release an expired task lease")
    reclaim.add_argument("task_id")
    add_agent_argument(reclaim)
    reclaim.add_argument("--reason", required=True)
    reclaim.set_defaults(handler=command_reclaim)

    queue = subparsers.add_parser("queue", help="move an unblocked task to queued")
    queue.add_argument("task_id")
    add_agent_argument(queue)
    queue.add_argument("--reason", required=True)
    queue.set_defaults(handler=command_queue)

    bind = subparsers.add_parser("bind-head", help="bind a non-running task head")
    bind.add_argument("task_id")
    add_agent_argument(bind)
    bind.add_argument("--head", required=True)
    bind.add_argument("--branch")
    bind.add_argument("--reason", required=True)
    bind.set_defaults(handler=command_bind_head)

    verify = subparsers.add_parser("verify", help="verify exact target head bindings")
    verify.add_argument("task_id", nargs="?")
    verify.set_defaults(handler=command_verify)

    doctor = subparsers.add_parser("doctor", help="check gh auth and registry access")
    doctor.set_defaults(handler=command_doctor)

    local = subparsers.add_parser("validate-local", help="validate a local registry")
    local.add_argument("file")
    local.set_defaults(handler=command_validate_local)

    render = subparsers.add_parser("render-local", help="render a local registry")
    render.add_argument("file")
    render.add_argument("--output")
    render.set_defaults(handler=command_render_local)

    self_test = subparsers.add_parser("self-test", help="run in-memory lock tests")
    self_test.set_defaults(handler=command_self_test)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    remote = RemoteRegistry(args.repo, args.coord_branch)
    try:
        args.handler(remote, args)
    except RemoteConflict as error:
        print(f"remote conflict: {error}", file=sys.stderr)
        return 3
    except (RegistryError, RuntimeError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
