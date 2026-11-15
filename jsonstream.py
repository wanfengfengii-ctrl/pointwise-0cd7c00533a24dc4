"""Incremental JSON framing only; no validation, canonicalization or storage policy."""
import json


class Reader:
    def __init__(self, source, chunk_size=16384):
        self.source = source
        self.chunk_size = chunk_size
        self.buffer = ""
        self.pos = 0
        self.eof = False
        self.decoder = json.JSONDecoder()

    def more(self):
        self.buffer = self.buffer[self.pos:]
        self.pos = 0
        text = self.source.read(self.chunk_size)
        if not text:
            self.eof = True
        self.buffer += text

    def char(self):
        while True:
            while self.pos < len(self.buffer):
                c = self.buffer[self.pos]
                if c in " \t\r\n":
                    self.pos += 1
                else:
                    return c
            if self.eof:
                return ""
            self.more()

    def take(self, expected):
        if self.char() != expected:
            raise ValueError("JSON_SYNTAX")
        self.pos += 1

    def value(self):
        if not self.char():
            raise ValueError("JSON_TRUNCATED")
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.pos)
            except json.JSONDecodeError:
                if self.eof:
                    raise ValueError("JSON_SYNTAX") from None
                self.more()
                continue
            # A number or constant may continue across the next read.
            if end == len(self.buffer) and not self.eof:
                self.more()
                continue
            if end < len(self.buffer) and self.buffer[end] not in " \t\r\n,:]}":
                if not self.eof:
                    self.more()
                    continue
                raise ValueError("JSON_SYNTAX")
            self.pos = end
            return value


def members(source, array_key="rows", chunk_size=16384):
    """Yield (kind,key,value): value; array, item*, end for the named array.
    source is an open text stream. Root must be an object; repeated keys are
    emitted in source order, without resolving duplicates. Items use ordinary
    json.loads semantics. The caller owns validation and last-member handling.
    Arbitrary order, whitespace and escaped member names are supported.
    """
    r = Reader(source, chunk_size)
    r.take("{")
    if r.char() == "}":
        r.pos += 1
    else:
        while True:
            key = r.value()
            if not isinstance(key, str):
                raise ValueError("JSON_KEY")
            r.take(":")
            if key == array_key and r.char() == "[":
                r.pos += 1
                yield ("array", key, None)
                if r.char() != "]":
                    while True:
                        yield ("item", key, r.value())
                        if r.char() == "]":
                            break
                        r.take(",")
                r.take("]")
                yield ("end", key, None)
            else:
                yield ("value", key, r.value())
            if r.char() == "}":
                r.pos += 1
                break
            r.take(",")
    if r.char():
        raise ValueError("JSON_TRAILING")
