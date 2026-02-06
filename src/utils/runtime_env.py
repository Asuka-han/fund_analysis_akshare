#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
runtime_env.py - helpers for locating paths in frozen and normal runs.

- get_repo_root: location of bundled code (sys._MEIPASS when frozen)
- get_storage_root: preferred place to read/write user data (exe dir when frozen)
- add_project_paths: ensure repo/storage/src are on sys.path
- chdir_to_storage: best-effort cwd normalization for onefile builds
"""

import os
import sys
from pathlib import Path
from typing import Tuple


def get_repo_root() -> Path:
    """Return the root where code is located (handles PyInstaller)."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent.parent.parent


def get_storage_root() -> Path:
    """Return directory for writable data (exe dir when frozen)."""
    override = os.getenv("FUND_ANALYSIS_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path.cwd()


def add_project_paths() -> Tuple[Path, Path]:
    """Ensure repo/storage/src are importable; return (repo_root, storage_root)."""
    repo_root = get_repo_root()
    storage_root = get_storage_root()
    for candidate in (storage_root, repo_root, repo_root / "src"):
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
    return repo_root, storage_root


def chdir_to_storage() -> Path:
    """Best effort to set cwd to storage root; safe if already there."""
    storage_root = get_storage_root()
    try:
        os.chdir(storage_root)
    except OSError:
        pass
    return storage_root
