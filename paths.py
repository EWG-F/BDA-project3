"""
TrendScope - Path Resolution
Single source of truth for every data/script location used by the app.

The modules live in ``src/``, so:

    BASE_DIR    = <repo>/src
    PARENT_DIR  = <repo>

Lookups are marker-based rather than hard-coded, which keeps the app working
whether the data sits in ``data/raw`` + ``data/processed`` (documented layout)
or directly next to the modules (flattened exports).
"""

import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(BASE_DIR)


def _pick(candidates, marker):
    """First candidate directory containing `marker`; else first existing; else first."""
    for directory in candidates:
        if directory and os.path.isdir(directory) and glob.glob(os.path.join(directory, marker)):
            return directory
    for directory in candidates:
        if directory and os.path.isdir(directory):
            return directory
    return candidates[0]


DATA_RAW_DIR = _pick(
    [
        os.path.join(PARENT_DIR, "data", "raw"),
        os.path.join(BASE_DIR, "data", "raw"),
        PARENT_DIR,
        BASE_DIR,
    ],
    "*videos.csv",
)

DATA_PROCESSED_DIR = _pick(
    [
        os.path.join(PARENT_DIR, "data", "processed"),
        os.path.join(BASE_DIR, "data", "processed"),
        PARENT_DIR,
        BASE_DIR,
    ],
    "mapreduce_job1_keywords.csv",
)

MAPREDUCE_DIR = _pick(
    [
        os.path.join(PARENT_DIR, "mapreduce"),
        os.path.join(BASE_DIR, "mapreduce"),
        PARENT_DIR,
        BASE_DIR,
    ],
    "job1_keyword_frequency",
)

HIVE_DIR = _pick(
    [
        os.path.join(PARENT_DIR, "hive"),
        os.path.join(BASE_DIR, "hive"),
        PARENT_DIR,
        BASE_DIR,
    ],
    "*.hql",
)


def resolve(directory, *names):
    """Resolve a filename that may live in `directory` or in the repo root / src/."""
    for name in names:
        for base in (directory, PARENT_DIR, BASE_DIR):
            candidate = os.path.join(base, name)
            if os.path.exists(candidate):
                return candidate
    return os.path.join(directory, names[0])
