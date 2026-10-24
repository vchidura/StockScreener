from __future__ import annotations

import json
import zlib


def decode_original_setup_payload(blob):
    if blob is None or len(blob) > 4_194_304:
        raise ValueError("original setup compressed payload exceeds bound")
    decoder = zlib.decompressobj()
    raw = decoder.decompress(blob, 16_777_217)
    if len(raw) > 16_777_216 or not decoder.eof or decoder.unused_data:
        raise ValueError(
            "original setup decompressed payload exceeds bound or is invalid"
        )
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("original setup payload must be an object")
    return payload
