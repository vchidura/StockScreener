from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
import os
from pathlib import Path
import re

from dotenv import dotenv_values


class Coordinate(ctypes.Structure):
    _fields_ = [("X", wintypes.SHORT), ("Y", wintypes.SHORT)]


class Rectangle(ctypes.Structure):
    _fields_ = [("Left", wintypes.SHORT), ("Top", wintypes.SHORT),
                ("Right", wintypes.SHORT), ("Bottom", wintypes.SHORT)]


class ConsoleInfo(ctypes.Structure):
    _fields_ = [("size", Coordinate), ("cursor", Coordinate),
                ("attributes", wintypes.WORD), ("window", Rectangle),
                ("maximum_window_size", Coordinate)]


def capture(process_id: int, lines: int) -> str:
    if os.name != "nt":
        raise RuntimeError("Windows console capture requires Windows")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                  wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.GetConsoleScreenBufferInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(ConsoleInfo)]
    kernel.ReadConsoleOutputCharacterW.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD,
                                                 Coordinate, ctypes.POINTER(wintypes.DWORD)]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.FreeConsole()
    handle = None
    try:
        if not kernel.AttachConsole(process_id):
            raise ctypes.WinError(ctypes.get_last_error())
        handle = kernel.CreateFileW("CONOUT$", 0x80000000, 3, None, 3, 0, None)
        if handle == wintypes.HANDLE(-1).value:
            handle = None
            raise ctypes.WinError(ctypes.get_last_error())
        info = ConsoleInfo()
        if not kernel.GetConsoleScreenBufferInfo(handle, ctypes.byref(info)):
            raise ctypes.WinError(ctypes.get_last_error())
        first_row = max(0, info.cursor.Y - lines + 1)
        count = min(200000, (info.cursor.Y - first_row + 1) * info.size.X)
        buffer = ctypes.create_unicode_buffer(count + 1)
        read = wintypes.DWORD()
        if not kernel.ReadConsoleOutputCharacterW(handle, buffer, count, Coordinate(0, first_row), ctypes.byref(read)):
            raise ctypes.WinError(ctypes.get_last_error())
        text = buffer[:read.value]
        return "\n".join(text[start:start + info.size.X].rstrip() for start in range(0, len(text), info.size.X))
    finally:
        if handle is not None:
            kernel.CloseHandle(handle)
        kernel.FreeConsole()
        kernel.AttachConsole(wintypes.DWORD(-1))


def main() -> int:
    parser = argparse.ArgumentParser(description="Read a bounded existing Windows console without signalling its process")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--lines", type=int, default=180)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.pid <= 0 or not 1 <= args.lines <= 500:
        parser.error("pid must be positive and lines must be between 1 and 500")
    if args.output.exists():
        parser.error("output already exists; preserve the previous capture")
    text = capture(args.pid, args.lines)
    values = {**dotenv_values(Path(__file__).resolve().parents[1] / ".env"), **os.environ}
    secrets = [value for key, value in values.items()
               if re.search(r"key|token|secret|password|credential", key, re.I) and value and len(value) >= 4]
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"(?i)((?:api[_-]?key|token|password|authorization)\s*[=:]\s*)[^\s&]+", r"\1[REDACTED]", text)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        output.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())