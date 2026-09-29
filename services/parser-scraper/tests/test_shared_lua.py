from pathlib import Path

import pytest

from app import shared_lua

SHARED = Path(__file__).resolve().parents[3] / "shared/redis"


@pytest.mark.parametrize("name,embedded", [
    ("jobs.lua", shared_lua.JOBS_LUA),
    ("reap.lua", shared_lua.REAP_LUA),
    ("enqueue_targets.lua", shared_lua.ENQUEUE_TARGETS_LUA),
])
def test_embedded_lua_matches_canonical(name, embedded):
    assert embedded.strip() == (SHARED / name).read_text().strip(), f"app/shared_lua.py drifted from shared/redis/{name}"
