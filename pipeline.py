"""Existing analysis kernel; deployment work must preserve these semantics."""
import hashlib
import io
import json
import os
import sqlite3
import tempfile

import jsonstream


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def validate(batch):
    if not isinstance(batch, dict) or set(batch) != {"batch_id", "scale", "rows"}:
        raise ValueError("BATCH")
    name = batch["batch_id"]
    if not isinstance(name, str) or not name or any(c not in
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in name):
        raise ValueError("BATCH_ID")
    if type(batch["scale"]) is not int or not 1 <= batch["scale"] <= 100:
        raise ValueError("SCALE")
    if not isinstance(batch["rows"], list):
        raise ValueError("ROWS")
    versions = {}
    for row in batch["rows"]:
        if not isinstance(row, dict) or set(row) != {
                "id", "rev", "station", "minute", "value", "deleted"}:
            raise ValueError("ROW")
        if any(not isinstance(row[k], str) or not row[k] for k in ("id", "station")):
            raise ValueError("IDENTITY")
        if type(row["rev"]) is not int or row["rev"] < 1:
            raise ValueError("REV")
        if any(type(row[k]) is not int for k in ("minute", "value")):
            raise ValueError("NUMBER")
        if type(row["deleted"]) is not bool:
            raise ValueError("DELETED")
        key = (row["id"], row["rev"])
        if key in versions and versions[key] != row:
            raise ValueError("REVISION_CONFLICT")
        versions[key] = row
    return batch


def stage(batch, output):
    validate(batch)
    latest = {}
    for row in batch["rows"]:
        if row["id"] not in latest or row["rev"] > latest[row["id"]]["rev"]:
            latest[row["id"]] = row
    for name in sorted(latest):
        row = latest[name]
        if not row["deleted"]:
            output.write(encode([row["id"], row["station"], row["minute"],
                                 row["value"] * batch["scale"]]))


def summarize(source):
    groups = {}
    for line in source:
        _, station, minute, value = json.loads(line)
        cell = groups.setdefault((station, minute), [0, 0])
        cell[0] += 1
        cell[1] += value
    return [{"station": station, "minute": minute, "count": value[0], "sum": value[1]}
            for (station, minute), value in sorted(groups.items())]


def receipt(batch_id, input_hash, result_bytes):
    return {"batch_id": batch_id, "input_sha256": input_hash,
            "result_sha256": hashlib.sha256(result_bytes).hexdigest()}


# --- Internal streaming file handoff -------------------------------------
# The object interfaces above are unchanged. The helpers below move the
# collector -> accept -> process chain to bounded-memory file handoff:
# two incremental passes over the input, an on-disk scratch revision store,
# and canonical bytes identical to encode(batch).

def canonical_bytes(value):
    """encode(value) without the trailing newline."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def open_text(path):
    """Decode a batch file exactly as json.loads(bytes) would, without
    materializing it; newline='' keeps the bytes' line endings untouched."""
    raw = open(path, "rb")
    return io.TextIOWrapper(raw, encoding=json.detect_encoding(raw.peek(4)[:4]),
                            newline="")


def scan_layout(source):
    """First pass: full JSON syntax check plus member layout. Non-winning
    rows arrays and overridden members are parsed but discarded; scalar
    members follow last-member-wins, mirroring json.loads."""
    keys = set()
    batch_id = scale = None
    rows_arrays = 0
    last_rows_array = False
    for kind, key, value in jsonstream.members(source):
        keys.add(key)
        if kind == "array":
            rows_arrays += 1
            last_rows_array = True
        elif kind == "value":
            if key == "rows":
                last_rows_array = False
            elif key == "batch_id":
                batch_id = value
            elif key == "scale":
                scale = value
    return {"keys": keys, "batch_id": batch_id, "scale": scale,
            "rows_arrays": rows_arrays, "last_rows_array": last_rows_array}


def validate_head(layout):
    """Batch-level checks, same order and errors as validate()."""
    if layout["keys"] != {"batch_id", "scale", "rows"}:
        raise ValueError("BATCH")
    name = layout["batch_id"]
    if not isinstance(name, str) or not name or any(c not in
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in name):
        raise ValueError("BATCH_ID")
    scale = layout["scale"]
    if type(scale) is not int or not 1 <= scale <= 100:
        raise ValueError("SCALE")
    if not layout["last_rows_array"]:
        raise ValueError("ROWS")
    return name, scale


def check_row(row):
    """Per-row field checks, identical to validate()."""
    if not isinstance(row, dict) or set(row) != {
            "id", "rev", "station", "minute", "value", "deleted"}:
        raise ValueError("ROW")
    if any(not isinstance(row[k], str) or not row[k] for k in ("id", "station")):
        raise ValueError("IDENTITY")
    if type(row["rev"]) is not int or row["rev"] < 1:
        raise ValueError("REV")
    if any(type(row[k]) is not int for k in ("minute", "value")):
        raise ValueError("NUMBER")
    if type(row["deleted"]) is not bool:
        raise ValueError("DELETED")


class Versions:
    """Scratch on-disk (id, rev) store with bounded cache; arbitrary
    precision revisions are kept as decimal text. cleanup() must run on
    completion, rejection, or failure."""

    def __init__(self, directory):
        fd, self.path = tempfile.mkstemp(prefix=".versions-", suffix=".sqlite",
                                         dir=str(directory))
        os.close(fd)
        self.db = None
        try:
            self.db = sqlite3.connect(self.path)
            self.db.execute("PRAGMA synchronous=OFF")
            self.db.execute("PRAGMA journal_mode=OFF")
            self.db.execute("PRAGMA temp_store=FILE")
            self.db.execute("PRAGMA cache_size=-2048")
            self.db.execute("CREATE TABLE versions(id TEXT NOT NULL, rev_len INTEGER NOT NULL,"
                            " rev_txt TEXT NOT NULL, row_json BLOB NOT NULL)")
            self.db.execute("CREATE UNIQUE INDEX versions_key ON versions(id, rev_txt)")
        except Exception:
            self.cleanup()
            raise

    def add(self, identity, revision, row_json):
        rev_txt = str(revision)
        old = self.db.execute("SELECT row_json FROM versions WHERE id=? AND rev_txt=?",
                              (identity, rev_txt)).fetchone()
        if old is not None:
            if old[0] != row_json:
                raise ValueError("REVISION_CONFLICT")
            return
        self.db.execute("INSERT INTO versions VALUES(?,?,?,?)",
                        (identity, len(rev_txt), rev_txt, row_json))

    def by_latest(self):
        # Positive integers order as (digit count, digits); BINARY collation
        # on UTF-8 text matches Python string order for ids.
        return self.db.execute("SELECT id, row_json FROM versions"
                               " ORDER BY id, rev_len DESC, rev_txt DESC")

    def cleanup(self):
        if self.db is not None:
            try:
                self.db.commit()
                self.db.close()
            except sqlite3.Error:
                pass
            self.db = None
        for suffix in ("", "-journal", "-wal"):
            try:
                os.unlink(self.path + suffix)
            except OSError:
                pass


def absorb_rows(path, layout, versions, sink=None):
    """Second pass: validate the winning rows array (the last rows member),
    record every revision, and optionally sink each canonical row in the
    original order. Only the last rows array is validated, matching
    json.loads last-member-wins plus validate() on the final object."""
    target = layout["rows_arrays"]
    ordinal = 0
    winning = False
    with open_text(path) as source:
        for kind, key, value in jsonstream.members(source):
            if kind == "array":
                ordinal += 1
                winning = ordinal == target
            elif kind == "item" and winning:
                check_row(value)
                line = canonical_bytes(value)
                versions.add(value["id"], value["rev"], line)
                if sink is not None:
                    sink(line)


def stage_versions(versions, scale, output):
    """Streaming stage(): highest revision per identity, tombstones dropped,
    identities in Python string order, exact integer value*scale."""
    previous = None
    for identity, row_json in versions.by_latest():
        if identity == previous:
            continue
        previous = identity
        row = json.loads(row_json)
        if row["deleted"]:
            continue
        output.write(encode([row["id"], row["station"], row["minute"],
                             row["value"] * scale]))
