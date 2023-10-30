import logging
import pytest

from http import HTTPStatus
from logging import config as logging_config

from tests.functional.settings import settings
from tests.functional.utils.logger import LOGGING

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)
pytestmark = pytest.mark.asyncio

PREFIX = '/api/v1/genres'


@pytest.mark.usefixtures('redis_clear_data_before_after',
                         'es_write_data')
class TestGenres:
    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}',
                    {'status': HTTPStatus.OK, 'length': 3, 'name': 'Action'}
            ),
        ]
    )
    async def test_get_all_genres(self,
                                  session_client,
                                  url,
                                  expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert len(body) == expected_answer['length']
            assert body[0]['name'] == expected_answer['name']
            assert list(body[0].keys()) == ['uuid', 'name']


@pytest.mark.usefixtures('redis_clear_data_before_after', 'es_write_data')
class TestGenreID:
    async def test_get_genre_by_id(self,
                                   session_client,
                                   get_id):
        _id = await get_id(f'{PREFIX}')
        expected_answer = {'status': HTTPStatus.OK,
                           'length': 2,
                           'name': 'Action'}
        url = settings.service_url + PREFIX + '/' + _id

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['name'] == expected_answer['name']
            assert len(body) == expected_answer['length']
            assert body['uuid'] == _id
            assert list(body.keys()) == ['uuid', 'name']

    async def test_get_genre_id_not_exists(self,
                                           session_client):

        url = settings.service_url + PREFIX + '/' + 'BAD_ID'
        expected_answer = {'status': HTTPStatus.NOT_FOUND}

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['detail'] == "BAD_ID not found in genres"


class TestGenresRedis:
    """
    The idea behind the Test class is:
    1. Run method `prepare`, that will load data to ES:
        1. Run HTTP request that will load data to redis
        2. Remove all data from ES
    2. Run method `get_from_redis`:
        1. Run HTTP request that will try to get data from redis
        2. Show the output
        3. Upload the data to redis
    """

    params_list = \
        [
            (
                f'{PREFIX}',
                {'status': HTTPStatus.OK, 'length': 3, 'name': 'Action'}
            )
        ]

    # This test only adds data to ES, adds data to redis, removes data from ES
    @pytest.mark.parametrize(
        'url, expected_answer',
        params_list
    )
    async def test_prepare_data(self,
                                redis_clear_data_before,
                                es_write_data,
                                session_client,
                                url,
                                expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            assert response.status == HTTPStatus.OK

    # This test DOESN'T add data to ES, but adds data to redis
    @pytest.mark.parametrize(
        'url, expected_answer',
        params_list
    )
    async def test_get_from_redis(self,
                                  redis_clear_data_after,
                                  session_client,
                                  url,
                                  expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert len(body) == expected_answer['length']
            assert sorted(body, key=lambda x: x['name'])[0]['name'] ==\
                   expected_answer['name']
            assert list(body[0].keys()) == ['uuid', 'name']


class TestGenreIdRedis:
    """
    The idea behind the Test class is:
    1. Run method `prepare`, that will load data to ES:
        1. Run HTTP request that will load data to redis
        2. Remove all data from ES
    2. Run method `get_from_redis`:
        1. Run HTTP request that will try to get data from redis
        2. Show the output
        3. Upload the data to redis
    """

    # This test only adds data to ES, adds data to redis, removes data from ES
    async def test_prepare_data(self,
                                redis_clear_data_before,
                                es_write_data,
                                session_client,
                                get_id):
        # Collect uuid
        global _id
        _id = await get_id(f'{PREFIX}')

        # Find data by id
        url = settings.service_url + PREFIX + '/' + _id

        async with session_client.get(url) as response:
            assert response.status == HTTPStatus.OK

    # This test DOESN'T add data to ES, but adds data to redis
    async def test_get_from_redis(self,
                                  redis_clear_data_after,
                                  session_client):

        expected_answer = {'status': HTTPStatus.OK,
                           'length': 2,
                           'name': 'Action'}
        try:
            url = settings.service_url + PREFIX + '/' + _id
        except NameError:
            logging.error(f"Can't run the test {PREFIX}/UUID with unknown id")
            assert False

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['name'] == expected_answer['name']
            assert len(body) == expected_answer['length']
            assert body['uuid'] == _id
            assert list(body.keys()) == ['uuid', 'name']
