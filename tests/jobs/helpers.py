"""Shared helpers for jobs tests."""

from __future__ import annotations


def descriptor(job_id: str, source_file: str) -> dict:
    return {"job_id": job_id, "source_file": source_file, "target": [10.0, 20.0], "size": 256}
