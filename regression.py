"""Business/rejection regression; no optimization is implemented here."""
import copy
import io
import json
from pathlib import Path
import random
import sqlite3
import sys
import tempfile

import bench
import checks
import collector
import jsonstream
import pipeline
import runtime


def reader_checks():
    samples = [
        '{}', '{"rows":[],"scale":1}',
        '{"rows":[1,-1,1.25,1e22,true,false,null,NaN,Infinity,-Infinity]}',
        '{"ro\\u0077s":[{"x":"站🛰\\n","x":2},[1,2]],"rows":[3]}',
        '{"x":{"rows":[1]},"rows":[10000000000000000000000000001]}'
    ]
    for text in samples:
        for width in (1, 2, 3, 7, 16384):
            restored = {}
            for kind, key, value in jsonstream.members(io.StringIO(text), chunk_size=width):
                if kind == "value":
                    restored[key] = value
                elif kind == "array":
                    restored[key] = []
                elif kind == "item":
                    restored[key].append(value)
            assert json.dumps(restored, sort_keys=True) == json.dumps(json.loads(text), sort_keys=True)
    for text in ('', '[]', '{"rows":[1,]}', '{"rows":[1}', '{}x', '{"x":trueX}'):
        for width in (1, 16384):
            try:
                list(jsonstream.members(io.StringIO(text), chunk_size=width))
            except ValueError:
                pass
            else:
                raise AssertionError(("BAD_JSON_ACCEPTED", text))
    return "READER_OK"


def ordinary(folder, text):
    folder.mkdir()
    raw = folder / "source.json"
    raw.write_text(text, encoding="utf-8")
    meta = bench.prepare(raw, folder)
    root = folder / "data"
    bench.initialize(root)
    collector.enqueue(root, "first", "http://fixture/export")
    assert bench.collect(root, raw, meta)
    actual = bench.assert_result(root, "first", meta)
    accepted = runtime.submit(root, json.loads(text))
    assert accepted == {"accepted": meta["batch_id"],
                        "input_sha256": meta["receipt"]["input_sha256"]}
    collector.enqueue(root, "repeat", "http://fixture/export")
    assert bench.collect(root, raw, meta)
    bench.assert_result(root, "repeat", meta)
    assert not runtime.run_one(root)
    changed = json.loads(text)
    changed["scale"] = 99 if changed["scale"] != 99 else 98
    try:
        runtime.submit(root, changed)
    except ValueError as error:
        assert str(error) == "BATCH_CONFLICT"
    else:
        raise AssertionError("BATCH_CONFLICT_MISSING")
    assert runtime.read(root, meta["batch_id"]) == actual
    return meta


def rejected(folder, text, expected_error=None):
    folder.mkdir()
    raw = folder / "source.json"
    raw.write_text(text, encoding="utf-8")
    meta = {"length": raw.stat().st_size, "raw_sha256": bench.digest(raw)}
    root = folder / "data"
    bench.initialize(root)
    collector.enqueue(root, "bad", "http://fixture/export")
    assert bench.collect(root, raw, meta)
    record = collector.status(root)[0]
    assert record["state"] == "failed", record
    if expected_error:
        assert record["error"] == expected_error, record
    assert not runtime.status(root)
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,)
    assert not list((root / "inputs").iterdir())
    assert not list((root / "work").iterdir())


def failure_retry(folder):
    folder.mkdir()
    raw = folder / "source.json"
    raw.write_bytes(pipeline.encode(checks.small()))
    meta = bench.prepare(raw, folder)
    root = folder / "data"
    bench.initialize(root)
    collector.enqueue(root, "retry", "http://fixture/export")

    class Broken(bench.FileConnection):
        def read(self, size):
            raise OSError("READ_INJECTED")

    assert bench.collect(root, raw, meta, lambda *a, **k: Broken(raw, meta, *a, **k))
    assert collector.status(root)[0]["state"] == "queued"
    assert not runtime.status(root)
    with runtime.connect(root) as db:
        db.executescript("""
CREATE TRIGGER fail_accounting BEFORE INSERT ON ledger
BEGIN SELECT RAISE(ABORT, 'COMMIT_INJECTED'); END;
""")
    try:
        bench.collect(root, raw, meta)
    except sqlite3.IntegrityError as error:
        assert "COMMIT_INJECTED" in str(error)
    else:
        raise AssertionError("ACCOUNTING_FAILURE_SWALLOWED")
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,)
        assert db.execute("SELECT state FROM jobs").fetchone() == ("running",)
        db.execute("DROP TRIGGER fail_accounting")
    # Commit failure leaves the retryable state and no new scratch files.
    assert sorted(p.name for p in (root / "inputs").iterdir()) == ["sample.json"]
    assert sorted(p.name for p in (root / "work").iterdir()) == ["sample.jsonl"]
    assert bench.collect(root, raw, meta)
    bench.assert_result(root, "retry", meta)
    return "READ_AND_COMMIT_RETRY_OK"


def assert_handoff_clean(root, name):
    # Only the durable handoff files remain; scratch stores and partial
    # canonical temporaries are always cleaned up.
    assert sorted(p.name for p in (Path(root) / "inputs").iterdir()) == [name + ".json"]
    assert sorted(p.name for p in (Path(root) / "work").iterdir()) == [name + ".jsonl"]


def recovered(folder):
    # A rejected download leaves the store ready for a good retry.
    folder.mkdir()
    root = folder / "data"
    bench.initialize(root)
    bad = checks.small()
    bad["rows"].append({"id": "z", "rev": 1, "station": "east"})
    raw = folder / "bad.json"
    raw.write_text(json.dumps(bad), encoding="utf-8")
    bad_meta = {"length": raw.stat().st_size, "raw_sha256": bench.digest(raw)}
    collector.enqueue(root, "bad", "http://fixture/export")
    assert bench.collect(root, raw, bad_meta)
    assert collector.status(root)[0]["state"] == "failed"
    assert not runtime.status(root)
    with runtime.connect(root) as db:
        assert db.execute("SELECT COUNT(*) FROM ledger").fetchone() == (0,)
    good = folder / "good.json"
    good.write_text(json.dumps(checks.small()), encoding="utf-8")
    meta = bench.prepare(good, folder)
    collector.enqueue(root, "good", "http://fixture/export")
    assert bench.collect(root, good, meta)
    bench.assert_result(root, "good", meta)
    assert_handoff_clean(root, "sample")
    return "RECOVERY_OK"


def boundary(base):
    base.mkdir()
    # Invalid tail rows must not be accepted; a valid prefix stays uncommitted.
    tail = checks.small()
    tail["rows"].append({"id": "z", "rev": 1, "station": "east"})
    rejected(base / "invalid_tail", json.dumps(tail), "ROW")
    conflict = checks.small()
    conflict["rows"].append(dict(conflict["rows"][0], value=12345))
    rejected(base / "tail_conflict", json.dumps(conflict), "REVISION_CONFLICT")
    # Differential edges vs the frozen reference: tombstone then resurrect,
    # zero-sum non-empty groups, arbitrary precision integers, exact
    # duplicate rows, non-ASCII identities.
    edge = {"batch_id": "edge", "scale": 100, "rows": [
        {"id": "a", "rev": 1, "station": "s", "minute": 0, "value": 1, "deleted": False},
        {"id": "a", "rev": 2, "station": "s", "minute": 0, "value": 2, "deleted": True},
        {"id": "a", "rev": 3, "station": "s", "minute": 0, "value": 7, "deleted": False},
        {"id": "b", "rev": 1, "station": "s", "minute": 0, "value": -7, "deleted": False},
        {"id": "c", "rev": 2 ** 80, "station": "站", "minute": -(2 ** 70),
         "value": 2 ** 100, "deleted": False},
        {"id": "c", "rev": 2 ** 80, "station": "站", "minute": -(2 ** 70),
         "value": 2 ** 100, "deleted": False},
        {"id": "d", "rev": 1, "station": "s", "minute": 0, "value": 3, "deleted": True},
    ]}
    ordinary(base / "edge", json.dumps(edge, ensure_ascii=False))
    assert_handoff_clean(base / "edge" / "data", "edge")
    # Overridden members: only the last rows array is validated and accepted,
    # earlier rows arrays still must be syntactically complete.
    overridden = ('{"rows":[{"bad":0}],"scale":false,"batch_id":"wrong","rows":'
                  + json.dumps(edge["rows"], ensure_ascii=False)
                  + ',"scale":5,"batch_id":"edge2"}')
    ordinary(base / "overridden", overridden)
    assert_handoff_clean(base / "overridden" / "data", "edge2")
    recovered(base / "recovered")
    return "BOUNDARY_OK"


def integration():
    rng = random.Random(217)
    with tempfile.TemporaryDirectory(prefix="capture-regression-") as temporary:
        base = Path(temporary)
        ordinary(base / "sample", json.dumps(checks.small(), ensure_ascii=True, indent=2))
        for case in range(8):
            rows = []
            for i in range(15):
                for revision in range(1, 4):
                    row = {"id": "观测🛰" + str(i), "rev": 2 ** 80 + revision,
                           "station": rng.choice(["东", "西", "ß"]),
                           "minute": rng.choice([-3, 0, 2 ** 80]),
                           "value": rng.choice([-2 ** 90, 0, 2 ** 100]),
                           "deleted": rng.choice([True, False])}
                    rows.append(row)
                    if i % 3 == 0:
                        rows.append(copy.deepcopy(row))
            rng.shuffle(rows)
            batch = {"batch_id": "random" + str(case), "scale": 7, "rows": rows}
            ordinary(base / ("random" + str(case)), json.dumps(batch, ensure_ascii=True))
        sample = checks.small()
        text = '{"rows":[{"bad":0}],"scale":false,"batch_id":"ignored",'
        text += '"rows":' + json.dumps(sample["rows"]) + ',"scale":10,"batch_id":"sample"}'
        ordinary(base / "duplicate_members", text)
        ordinary(base / "empty", '{"rows":[],"scale":1,"batch_id":"empty"}')
        bad = checks.small()
        bad["rows"].append(dict(bad["rows"][0], value=12345))
        rejected(base / "old_conflict", json.dumps(bad), "REVISION_CONFLICT")
        rejected(base / "truncated", json.dumps(sample)[:-2])
        bad = checks.small()
        bad["rows"].append(dict(bad["rows"][0], value=True))
        rejected(base / "boolean", json.dumps(bad), "NUMBER")
        retry = failure_retry(base / "retry")
        edges = boundary(base / "boundary")
    return {"marker": "INTEGRATION_OK", "retry": retry, "boundary": edges}


def verify(mode):
    assert mode in ("baseline", "target")
    value = {"business": checks.normal(), "source": checks.source(),
             "reader": reader_checks(), "integration": integration()}
    value["benchmark"] = bench.measure(mode)
    return value


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) == 2 else ""
    if mode not in ("baseline", "target", "quick"):
        raise SystemExit("usage: regression.py baseline|target|quick")
    value = ({"business": checks.normal(), "source": checks.source(),
              "reader": reader_checks(), "integration": integration()}
             if mode == "quick" else verify(mode))
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))
