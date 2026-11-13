"""Unit tests for the incremental bookkeeping in src/transform/gold_job.py.

Only the pure key parser is covered here: the rest of the module talks to S3
and JDBC. The parser is what decides whether a Silver file counts as
processed, so a wrong answer would either reprocess everything or silently
skip a month.
"""

import pytest

from src.transform.gold_job import pair_from_manifest_key

VALID = "_manifests/aggtrades/symbol=BTCUSDT/2026-06.json"


def test_valid_manifest_key():
    assert pair_from_manifest_key(VALID) == ("BTCUSDT", "2026-06")


@pytest.mark.parametrize("key", [
    "_manifests/aggtrades/symbol=BTCUSDT/2026-06.parquet",  # not a manifest
    "_manifests/aggtrades/BTCUSDT/2026-06.json",            # no symbol= prefix
    "_manifests/aggtrades/symbol=/2026-06.json",            # empty symbol
    "_manifests/aggtrades/symbol=BTCUSDT/.json",            # empty month
    "_manifests/aggtrades/symbol=BTCUSDT/extra/2026-06.json",
    "_manifests/aggtrades/2026-06.json",
    "",
])
def test_anything_else_is_ignored(key):
    assert pair_from_manifest_key(key) is None
