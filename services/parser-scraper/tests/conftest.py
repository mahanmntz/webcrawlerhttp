import os

import pytest
import redis


@pytest.fixture
def rdb():
    """A real Redis on a dedicated DB, flushed around each test.

    Skips if Redis is unreachable, unless REQUIRE_REDIS is set (CI).
    """
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_TEST_DB", "11")),
        decode_responses=True,
        socket_connect_timeout=1,
    )
    try:
        client.ping()
    except redis.ConnectionError as err:
        if os.getenv("REQUIRE_REDIS"):
            pytest.fail(f"Redis required but not reachable: {err}")
        pytest.skip(f"Redis not reachable: {err}")

    client.flushdb()
    yield client
    client.flushdb()
    client.close()
