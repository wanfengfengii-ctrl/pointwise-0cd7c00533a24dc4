"""Frozen full-chain workloads. Fixture/oracle preparation is outside the child."""
import functools
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import tracemalloc

import collector
import reference
import runtime

MIB = 1024 * 1024
SIZES = (100000, 400000)
KINDS = ("distinct", "hot")


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        while True:
            block = source.read(65536)
            if not block:
                return value.hexdigest()
            value.update(block)


def equal_files(left, right):
    with Path(left).open("rb") as a, Path(right).open("rb") as b:
        while True:
            x, y = a.read(65536), b.read(65536)
            assert x == y, "BYTES_CHANGED"
            if not x:
                return


def fixture(path, count, kind):
    # Fixed-size identities, bounded fields, <=256 output groups.
    with Path(path).open("w", encoding="utf-8", newline="") as out:
        out.write(' { "scale":3, "rows":[\n')
        for i in range(count):
            identity = i if kind == "distinct" else i % 8
            row = {"id": "r%09d" % identity,
                   "rev": 1 if kind == "distinct" else 1000000 + (i * 104729) % count,
                   "station": "站%02d" % (i % 16), "minute": (i // 16) % 16,
                   "value": i % 19 - 9, "deleted": i % 13 == 0}
            if i:
                out.write(",\n")
            out.write(json.dumps(row, ensure_ascii=True))
        out.write('], "batch_id":' + json.dumps(kind + str(count)) + " } \n")


def prepare(raw, folder):
    folder = Path(folder)
    canonical, result = folder / "oracle.input", folder / "oracle.result"
    meta = reference.inspect_file(raw, canonical, result)
    meta.update({"raw": str(Path(raw).resolve()), "canonical": str(canonical.resolve()),
                 "result": str(result.resolve())})
    return meta


class FileConnection:
    """An immutable file-backed HTTP response; the body is never materialized."""
    def __init__(self, raw, meta, *args, **kwargs):
        self.raw, self.meta = Path(raw), meta
        self.source = None
        self.status = 200

    def request(self, method, path, headers=None):
        assert method == "GET" and path == "/export"
        assert headers == {"Accept-Encoding": "identity"}

    def getresponse(self):
        self.source = self.raw.open("rb")
        return self

    def getheader(self, key, default=None):
        return {"ETag": '"frozen"', "X-Content-SHA256": self.meta["raw_sha256"],
                "Content-Length": str(self.meta["length"])}.get(key, default)

    def read(self, size):
        return self.source.read(size)

    def close(self):
        if self.source is not None:
            self.source.close()


def collect(root, raw, meta, factory=None):
    original = collector.download
    connection = factory or functools.partial(FileConnection, raw, meta)

    def download(url, target, progress=None, connection_factory=None):
        return original(url, target, progress, connection_factory or connection)

    collector.download = download
    try:
        return collector.run_one(root)
    finally:
        collector.download = original


def initialize(root):
    root = Path(root)
    runtime.init(root, root.parent / (root.name + "-control"))
    collector.init(root)


def assert_result(root, task, meta):
    root = Path(root)
    name = meta["batch_id"]
    state = collector.status(root)
    record = next(row for row in state if row["task_id"] == task)
    assert record["state"] == "completed", record
    assert record["received"] == record["length"] == meta["length"]
    assert record["sha256"] == meta["raw_sha256"]
    assert record["receipt"] == meta["receipt"]
    equal_files(root / "downloads" / (task + ".raw"), meta["raw"])
    equal_files(root / "inputs" / (name + ".json"), meta["canonical"])
    equal_files(root / "results" / (name + ".json"), meta["result"])
    result = runtime.read(root, name)
    assert result["available"] and result["receipt"] == meta["receipt"]
    assert result["result"] == json.loads(Path(meta["result"]).read_bytes())
    assert runtime.status(root) == [{"batch_id": name, "state": "completed",
                                      "receipt": meta["receipt"]}]
    with runtime.connect(root) as db:
        records = db.execute("SELECT id,result,receipt FROM ledger").fetchall()
    assert len(records) == 1 and records[0][0] == name
    assert records[0][1] == Path(meta["result"]).read_bytes()
    assert json.loads(records[0][2]) == meta["receipt"]
    return result


def child(meta_path, root):
    # Imports, DB initialization, source and reference generation are unmeasured.
    meta = json.loads(Path(meta_path).read_bytes())
    root = Path(root)
    initialize(root)
    collector.enqueue(root, "capture", "http://fixture/export")
    tracemalloc.start()
    started = time.perf_counter()
    assert collect(root, meta["raw"], meta)
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert_result(root, "capture", meta)
    print(json.dumps({"peak_bytes": peak, "elapsed_seconds": elapsed,
                      "rows": meta["rows"], "kind": meta["kind"],
                      "receipt": meta["receipt"], "raw_sha256": meta["raw_sha256"]},
                     ensure_ascii=False, sort_keys=True))


def one_case(folder, count, kind):
    folder = Path(folder)
    folder.mkdir()
    raw = folder / "source.json"
    fixture(raw, count, kind)
    meta = prepare(raw, folder)
    meta.update({"rows": count, "kind": kind})
    meta_path = folder / "meta.json"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    completed = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                                "--child", str(meta_path), str(folder / "data")],
                               capture_output=True, text=True)
    if completed.returncode:
        raise AssertionError(completed.stdout + completed.stderr)
    return json.loads(completed.stdout)


def measure(mode):
    reports = []
    with tempfile.TemporaryDirectory(prefix="capture-bench-") as temporary:
        for kind in KINDS:
            pair = [one_case(Path(temporary) / (kind + str(n)), n, kind) for n in SIZES]
            if mode == "target":
                assert all(row["peak_bytes"] <= 32 * MIB for row in pair), ("FULL_CHAIN_MEMORY_LIMIT", pair)
                assert pair[1]["peak_bytes"] - pair[0]["peak_bytes"] <= 4 * MIB, ("FULL_CHAIN_MEMORY_GROWTH", pair)
            reports.extend(pair)
    return {"marker": "FULL_CHAIN_" + mode.upper() + "_OK", "measurements": reports}


def demo():
    from checks import small
    with tempfile.TemporaryDirectory(prefix="capture-demo-") as temporary:
        folder = Path(temporary)
        raw = folder / "source.json"
        raw.write_text(json.dumps(small(), ensure_ascii=True, indent=2) + " \n",
                       encoding="utf-8")
        meta = prepare(raw, folder)
        root = folder / "data"
        initialize(root)
        collector.enqueue(root, "demo", "http://fixture/export")
        assert collect(root, raw, meta)
        result = assert_result(root, "demo", meta)
        return {"download": collector.status(root)[0], "business": result,
                "raw_and_canonical_differ":
                    meta["raw_sha256"] != meta["receipt"]["input_sha256"]}


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--child":
        child(sys.argv[2], sys.argv[3])
    else:
        mode = sys.argv[1] if len(sys.argv) == 2 else ""
        if mode not in ("baseline", "target", "demo"):
            raise SystemExit("usage: bench.py baseline|target|demo")
        print(json.dumps(demo() if mode == "demo" else measure(mode),
                         ensure_ascii=False, sort_keys=True))
