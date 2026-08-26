from __future__ import annotations

# Must be set before any fornax_cutouts import triggers CONFIG instantiation.
import os

os.environ.setdefault("CUTOUTS__SOURCE_PATH", "/tmp")

import pytest
import redis
import redis.asyncio as aioredis

# Use DB 15 to stay well clear of any local dev data.
_REDIS_URL = "redis://localhost:6379/15"


@pytest.fixture
def sync_redis():
    client = redis.Redis.from_url(_REDIS_URL, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
async def async_redis():
    client = aioredis.Redis.from_url(_REDIS_URL, decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()
