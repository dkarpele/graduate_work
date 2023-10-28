import pytest_asyncio

from tests.functional.settings import settings


@pytest_asyncio.fixture(scope='function')
async def get_id(session_client):
    async def inner(prefix: str):
        url = settings.service_url + prefix

        async with session_client.get(url) as response:
            body = await response.json()
            return body[0]['uuid']
    yield inner


@pytest_asyncio.fixture(scope='function')
async def get_token(session_client):
    async def inner(payload: dict, token_type: str = 'access'):
        prefix = '/api/v1/auth'
        postfix = '/login'

        url = settings.auth_url + prefix + postfix

        async with session_client.post(url, data=payload) as response:
            body = await response.json()
            if token_type == 'access':
                return body['access_token']
            if token_type == 'refresh':
                return body['refresh_token']

    yield inner
