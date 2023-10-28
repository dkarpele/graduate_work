import aiohttp
import asyncio

import pytest_asyncio
import typer
from elasticsearch import AsyncElasticsearch

from tests.functional.settings import settings

pytest_plugins = ("tests.functional.fixtures.get_data",
                  "tests.functional.fixtures.redis",
                  "tests.functional.fixtures.es",)


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(
        hosts=f'{settings.elastic_host}:{settings.elastic_port}')
    yield client
    await client.close()


@pytest_asyncio.fixture(scope='session')
async def session_client():
    client = aiohttp.ClientSession()
    yield client
    await client.close()
