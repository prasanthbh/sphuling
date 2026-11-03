"""Resource parsing utilities for CPU cores and memory bounds."""

from __future__ import annotations
import os
import re
from typing import Optional


def parse_memory_to_bytes(memory_str: Optional[str], default_bytes: int = 4 * 1024 * 1024 * 1024) -> int:
    """Parses a human-readable memory string (e.g., '16g', '512m', '2048k', '1073741824')
    into integer bytes.
    """
    if not memory_str:
        return default_bytes
    
    clean_str = memory_str.strip().lower()
    units = {
        "k": 1024,
        "kb": 1024,
        "m": 1024 * 1024,
        "mb": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "t": 1024 * 1024 * 1024 * 1024,
        "tb": 1024 * 1024 * 1024 * 1024,
    }
    
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([a-z]+)?$", clean_str)
    if not match:
        try:
            return int(float(clean_str))
        except ValueError:
            return default_bytes
    
    val = float(match.group(1))
    unit = match.group(2)
    
    if unit in units:
        return int(val * units[unit])
    return int(val)


def parse_cores(master: Optional[str], configured_cores: Optional[str] = None) -> int:
    """Calculates effective number of executor cores from master string
    (e.g., 'local', 'local[4]', 'local[*]') or explicit 'sphuling.executor.cores'.
    """
    if configured_cores:
        try:
            return max(1, int(configured_cores))
        except ValueError:
            pass

    if not master:
        return max(1, os.cpu_count() or 4)

    master = master.strip()
    if master == "local":
        return 1
    if master == "local[*]":
        return max(1, os.cpu_count() or 4)
    
    match = re.match(r"^local\[([0-9]+)\]$", master)
    if match:
        return max(1, int(match.group(1)))

    return max(1, os.cpu_count() or 4)

