import pytest_asyncio
import redis

from tests.functional.settings import settings


@pytest_asyncio.fixture(scope='class')
async def redis_clear_data_before_after():
    redis_cli = redis.Redis(host=settings.redis_host,
                            port=settings.redis_port)
    redis_cli.flushall()
    yield
    redis_cli.flushall()


@pytest_asyncio.fixture(scope='class')
async def redis_clear_data_after():
    redis_cli = redis.Redis(host=settings.redis_host,
                            port=settings.redis_port)
    yield
    redis_cli.flushall()


@pytest_asyncio.fixture(scope='class')
async def redis_clear_data_before():
    redis_cli = redis.Redis(host=settings.redis_host,
                            port=settings.redis_port)
    redis_cli.flushall()
    yield


