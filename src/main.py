"""Fixed-coordinator replicated KV with real replica processes and small-history checking."""

import argparse
import itertools
import json
import multiprocessing as mp
from multiprocessing.connection import Connection
import threading
import time
from pathlib import Path


def replica(pipe: Connection) -> None:
    pipe.send(("ready",))
    prepared = {}
    committed = {}
    while True:
        seq, kind, payload, delay = pipe.recv()
        time.sleep(delay)
        if kind == "prepare":
            prepared[seq] = payload
        elif kind == "commit" and seq in prepared:
            key, value = prepared.pop(seq)
            committed[key] = value
        pipe.send((seq, kind))


class Cluster:
    def __init__(self) -> None:
        self.ctx = mp.get_context("spawn")
        self.nodes = []
        self.pipes = []
        self.state = {}
        self.seq = 0
        self.lock = threading.Lock()
        self.blocked = set()
        self.slow = set()
        for _ in range(3):
            parent, child = self.ctx.Pipe()
            process = self.ctx.Process(target=replica, args=(child,), daemon=True)
            process.start()
            child.close()
            self.nodes.append(process)
            self.pipes.append(parent)

        for pipe in self.pipes:
            if not pipe.poll(10) or pipe.recv() != ("ready",):
                self.close()
                raise RuntimeError("replica startup failed")

    def put(self, key: str, value: int) -> bool:
        with self.lock:
            self.seq += 1
            seq = self.seq
            accepted = []
            for i, (process, pipe) in enumerate(zip(self.nodes, self.pipes)):
                if not process.is_alive() or i in self.blocked:
                    continue
                pipe.send((seq, "prepare", (key, value), 0.15 if i in self.slow else 0))
            for i, (process, pipe) in enumerate(zip(self.nodes, self.pipes)):
                if not process.is_alive() or i in self.blocked:
                    continue
                deadline = time.monotonic() + 0.05
                while time.monotonic() < deadline and pipe.poll(
                    max(0, deadline - time.monotonic())
                ):
                    response = pipe.recv()
                    if response == (seq, "prepare"):
                        accepted.append(i)
                        break
            if len(accepted) < 2:
                return False
            self.state[key] = value
            for i in accepted:
                self.pipes[i].send((seq, "commit", None, 0))
            return True

    def get(self, key: str, bug: bool = False) -> int | None:
        with self.lock:
            return 0 if bug else self.state.get(key)

    def close(self) -> None:
        for process in self.nodes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=3)
        for pipe in self.pipes:
            pipe.close()


def linearizable(history: list[dict]) -> bool:
    """Exhaust all total orders consistent with observed real-time precedence."""
    for order in itertools.permutations(range(len(history))):
        position = {operation: i for i, operation in enumerate(order)}
        if any(
            a["end"] < b["start"] and position[i] > position[j]
            for i, a in enumerate(history)
            for j, b in enumerate(history)
        ):
            continue
        state = {}
        valid = True
        for i in order:
            op = history[i]
            if op["kind"] == "put":
                state[op["key"]] = op["value"]
            elif state.get(op["key"]) != op["value"]:
                valid = False
                break
        if valid:
            return True
    return False


def benchmark() -> dict:
    cluster = Cluster()
    history = []
    try:
        for value in range(3):
            start = time.monotonic()
            assert cluster.put("x", value)
            history.append(
                {"kind": "put", "key": "x", "value": value, "start": start, "end": time.monotonic()}
            )
            start = time.monotonic()
            actual = cluster.get("x")
            history.append(
                {
                    "kind": "get",
                    "key": "x",
                    "value": actual,
                    "start": start,
                    "end": time.monotonic(),
                }
            )
        assert linearizable(history)
        cluster.nodes[0].terminate()
        cluster.nodes[0].join()
        start = time.monotonic()
        assert cluster.put("x", 3)
        buggy = [{"kind": "put", "key": "x", "value": 3, "start": start, "end": time.monotonic()}]
        cluster.blocked = {1}
        assert not cluster.put("x", 99)
        assert cluster.get("x") == 3
        cluster.blocked = set()
        cluster.slow = {1}
        assert not cluster.put("x", 99)
        assert cluster.get("x") == 3
        start = time.monotonic()
        actual_buggy_value = cluster.get("x", bug=True)
        buggy.append(
            {
                "kind": "get",
                "key": "x",
                "value": actual_buggy_value,
                "start": start,
                "end": time.monotonic(),
            }
        )
        assert not linearizable(buggy)
        Path("evidence/history.json").write_text(json.dumps(history, indent=2))
        Path("evidence/seeded-bug.json").write_text(json.dumps(buggy, indent=2))
        return {
            "replica_processes": 3,
            "checked_operations": len(history),
            "fault_scenarios_passed": 3,
            "seeded_bug_detected": True,
        }
    finally:
        cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-bug", action="store_true")
    args = parser.parse_args()
    if args.seed_bug:
        history = json.loads(Path("evidence/seeded-bug.json").read_text())
        ok = linearizable(history)
        print(json.dumps({"linearizable": ok}))
        raise SystemExit(0 if ok else 1)
    print(json.dumps(benchmark(), indent=2))
