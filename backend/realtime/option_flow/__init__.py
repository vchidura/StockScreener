"""Offline options flow engine: opening flow and block detection (task D).

Pure and deterministic. The only I/O is the PostgreSQL writer (writer.py) and the
condition-reference loaders in conditions.py, which read fixture files.
"""
