import logging

import aiohttp
import pytest

from http import HTTPStatus
from logging import config as logging_config

from tests.functional.settings import settings
from tests.functional.utils.logger import LOGGING

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)
pytestmark = pytest.mark.asyncio

PREFIX = '/api/v1/films'


@pytest.mark.usefixtures('redis_clear_data_before_after',
                         'es_write_data')
class TestFilms:
    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?genre=Action',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?genre=Music Story',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?genre=music',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?page_size=5',
                    {'status': HTTPStatus.OK, 'length': 5, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?page_number=2',
                    {'status': HTTPStatus.OK, 'length': 60 - 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?page_number=4&page_size=5',
                    {'status': HTTPStatus.OK, 'length': 5, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/?page_number=4&page_size=5&genre=Action',
                    {'status': HTTPStatus.OK, 'length': 5, 'title': 'The Star'}
            ),
        ]
    )
    async def test_get_all_films(self,
                                 session_client,
                                 url,
                                 expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert len(body) == expected_answer['length']
            assert body[0]['title'] == expected_answer['title']
            assert list(body[0].keys()) == ['uuid', 'title', 'imdb_rating']
            assert type(body[0]['imdb_rating']) == float

    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}/?genre=doesntexist',
                    {'status': HTTPStatus.NOT_FOUND, 'detail': 'movies not found'}
            ),
            (
                    f'{PREFIX}/?page_number=4&page_size=5&genre=doesntexist',
                    {'status': HTTPStatus.NOT_FOUND, 'detail': 'movies not found'}
            ),
        ]
    )
    async def test_get_all_films_not_found(self,
                                           session_client,
                                           url,
                                           expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['detail'] == expected_answer['detail']

    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}/?page_size=-5',
                    {'status': HTTPStatus.UNPROCESSABLE_ENTITY,
                     'type0': 'value_error.number.not_ge',
                     'msg': 'greater than or equal to 1'}
            ),
            (
                    f'{PREFIX}/?page_size=500000000000000',
                    {'status': HTTPStatus.UNPROCESSABLE_ENTITY,
                     'type0': 'value_error.number.not_le',
                     'msg': 'less than or equal to 500'}
            ),
            (
                    f'{PREFIX}/?page_number=-5',
                    {'status': HTTPStatus.UNPROCESSABLE_ENTITY,
                     'type0': 'value_error.number.not_ge',
                     'msg': 'greater than or equal to 1'}
            ),
            (
                    f'{PREFIX}/?page_number=5000000000000&genre=Action',
                    {'status': HTTPStatus.UNPROCESSABLE_ENTITY,
                     'type0': 'value_error.number.not_le',
                     'msg': 'less than or equal to 10000'}
            ),
            (
                    f'{PREFIX}/?page_number=-4&page_size=5000000000',
                    {'status': HTTPStatus.UNPROCESSABLE_ENTITY,
                     'type0': 'value_error.number.not_ge',
                     'msg': 'greater than or equal to 1',
                     'type1': 'value_error.number.not_le'}
            ),
            (
                    f'{PREFIX}/?page_number=fff&page_size=pop',
                    {'status': HTTPStatus.UNPROCESSABLE_ENTITY,
                     'type0': 'type_error.integer',
                     'msg': 'value is not a valid integer',
                     'type1': 'type_error.integer'}
            )
        ]
    )
    async def test_get_all_films_pagination_negative(self,
                                                     session_client,
                                                     url,
                                                     expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['detail'][0]['type'] == expected_answer['type0']
            assert expected_answer['msg'] in body['detail'][0]['msg']
            try:
                assert body['detail'][1]['type'] == expected_answer['type1']
            except (KeyError, IndexError):
                pass

    @pytest.mark.parametrize(
        'url, expected_answer, reverse',
        [
            (
                    f'{PREFIX}/?sort=imdb_rating',
                    {'status': HTTPStatus.OK},
                    False
            ),
            (
                    f'{PREFIX}/?sort=-imdb_rating',
                    {'status': HTTPStatus.OK},
                    True
            ),
            (
                    f'{PREFIX}/search?query=Star&sort=-imdb_rating',
                    {'status': HTTPStatus.OK},
                    True
            )
        ]
    )
    async def test_get_all_films_sort(self,
                                      session_client,
                                      url,
                                      expected_answer,
                                      reverse):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            imdb_rating_list = [i['imdb_rating'] for i in body]
            assert sorted(imdb_rating_list, reverse=reverse) == \
                   imdb_rating_list

    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}/?sort=doesntexist',
                    {'status': HTTPStatus.NOT_FOUND,
                     'detail': 'movies not found'}
            ),
            (
                    f'{PREFIX}/search?query=doesntexist&sort=imdb_rating',
                    {'status': HTTPStatus.NOT_FOUND,
                     'detail': 'movies not found'}
            )
        ]
    )
    async def test_get_all_films_sort_not_found(self,
                                                session_client,
                                                url,
                                                expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['detail'] == expected_answer['detail']


@pytest.mark.usefixtures('redis_clear_data_before_after', 'es_write_data')
@pytest.mark.xfail(reason="It fails if admin user doesn't exist in DB or "
                          "auth server isn't running")
class TestFilmID:
    async def test_get_film_by_id(self,
                                  get_token,
                                  get_id):
        access_data = {"username": "admin@example.com",
                       "password": "Secret123"}
        access_token = await get_token(access_data)
        header = {'Authorization': f'Bearer {access_token}'}

        _id = await get_id(f'{PREFIX}/?page_size=1')
        expected_answer = {'status': HTTPStatus.OK, 'length': 8, 'title': 'The Star'}
        url = settings.service_url + PREFIX + '/' + _id

        async with aiohttp.ClientSession(headers=header) as session:
            async with session.get(url) as response:
                assert response.status == expected_answer['status']
                body = await response.json()

                assert body['title'] == expected_answer['title']
                assert len(body) == expected_answer['length']
                assert body['uuid'] == _id
                assert list(body.keys()) == ['uuid', 'title', 'imdb_rating',
                                             'description', 'genre', 'actors',
                                             'writers', 'directors']

    async def test_get_film_id_not_exists(self,
                                          get_token):
        access_data = {"username": "admin@example.com",
                       "password": "Secret123"}
        access_token = await get_token(access_data)
        header = {'Authorization': f'Bearer {access_token}'}

        url = settings.service_url + PREFIX + '/' + 'BAD_ID'
        expected_answer = {'status': HTTPStatus.NOT_FOUND}

        async with aiohttp.ClientSession(headers=header) as session:
            async with session.get(url) as response:
                body = await response.json()

                assert response.status == expected_answer['status']
                assert body['detail'] == "BAD_ID not found in movies"


@pytest.mark.usefixtures('redis_clear_data_before_after', 'es_write_data')
class TestFilmSearch:
    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}/search?query=Star',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/search?query=The Star',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/search?query=star',
                    {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                    f'{PREFIX}/search?query=Star&page_number=4&page_size=5',
                    {'status': HTTPStatus.OK, 'length': 5, 'title': 'The Star'}
            ),
        ]
    )
    async def test_search_films(self,
                                session_client,
                                url,
                                expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert len(body) == expected_answer['length']
            assert body[0]['title'] == expected_answer['title']
            assert list(body[0].keys()) == ['uuid', 'title', 'imdb_rating']
            assert type(body[0]['imdb_rating']) == float

    async def test_search_films_pagination_negative(self,
                                                    session_client):
        url = settings.service_url + \
              f'{PREFIX}/search?query=Star&page_number=4&page_size=5000'

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == HTTPStatus.UNPROCESSABLE_ENTITY
            assert body['detail'][0]['type'] == 'value_error.number.not_le'
            assert 'less than or equal to 500' in body['detail'][0]['msg']

    @pytest.mark.parametrize(
        'url, expected_answer',
        [
            (
                    f'{PREFIX}/search?query=doesntexist',
                    {'status': HTTPStatus.NOT_FOUND,
                     'detail': 'movies not found'}
            ),
            (
                    f'{PREFIX}/search?query=',
                    {'status': HTTPStatus.NOT_FOUND,
                     'detail': 'Empty `query` attribute'}
            ),
        ]
    )
    async def test_search_films_not_found(self,
                                          session_client,
                                          url,
                                          expected_answer):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            assert body['detail'] == expected_answer['detail']


class TestFilmsRedis:
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
                f'{PREFIX}/?genre=Action',
                {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                f'{PREFIX}/?page_number=4&page_size=5&genre=Action',
                {'status': HTTPStatus.OK, 'length': 5, 'title': 'The Star'}
            ),
            (
                f'{PREFIX}/search?query=Star',
                {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                f'{PREFIX}/search?query=The Star',
                {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'}
            ),
            (
                f'{PREFIX}/search?query=Star&page_number=4&page_size=5',
                {'status': HTTPStatus.OK, 'length': 5, 'title': 'The Star'}
            ),
            (
                f'{PREFIX}/search?query=Star&page_number=4&page_size=10',
                {'status': HTTPStatus.OK, 'length': 10, 'title': 'The Star'}
            ),
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
            assert body[0]['title'] == expected_answer['title']
            assert list(body[0].keys()) == ['uuid', 'title', 'imdb_rating']
            assert type(body[0]['imdb_rating']) == float


class TestFilmsSortRedis:
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
                f'{PREFIX}/search?query=Star&sort=imdb_rating',
                {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'},
                False
            ),
            (
                f'{PREFIX}/search?query=Star&sort=-imdb_rating',
                {'status': HTTPStatus.OK, 'length': 50, 'title': 'The Star'},
                True
            ),
        ]

    # This test only adds data to ES, adds data to redis, removes data from ES
    @pytest.mark.parametrize(
        'url, expected_answer, reverse',
        params_list
    )
    async def test_prepare_data(self,
                                redis_clear_data_before,
                                es_write_data,
                                session_client,
                                url,
                                expected_answer,
                                reverse):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            assert response.status == HTTPStatus.OK

    @pytest.mark.parametrize(
        'url, expected_answer, reverse',
        params_list
    )
    async def test_get_from_redis(self,
                                  redis_clear_data_after,
                                  session_client,
                                  url,
                                  expected_answer,
                                  reverse):
        url = settings.service_url + url

        async with session_client.get(url) as response:
            body = await response.json()

            assert response.status == expected_answer['status']
            imdb_rating_list = [i['imdb_rating'] for i in body]
            assert sorted(imdb_rating_list, reverse=reverse) == \
                   imdb_rating_list


@pytest.mark.xfail(reason="It fails if admin user doesn't exist in DB or "
                          "auth server isn't running")
class TestFilmIdRedis:
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
                                get_id,
                                get_token):
        access_data = {"username": "admin@example.com",
                       "password": "Secret123"}
        access_token = await get_token(access_data)
        header = {'Authorization': f'Bearer {access_token}'}

        # Collect film uuid
        global _id
        _id = await get_id(f'{PREFIX}/?page_size=1')

        # Find data by id
        url = settings.service_url + PREFIX + '/' + _id

        async with aiohttp.ClientSession(headers=header) as session:
            async with session.get(url) as response:
                assert response.status == HTTPStatus.OK

    # This test DOESN'T add data to ES, but adds data to redis
    async def test_get_from_redis(self,
                                  redis_clear_data_after,
                                  get_token):
        access_data = {"username": "admin@example.com",
                       "password": "Secret123"}
        access_token = await get_token(access_data)
        header = {'Authorization': f'Bearer {access_token}'}

        expected_answer = {'status': HTTPStatus.OK,
                           'length': 8,
                           'title': 'The Star'}
        try:
            url = settings.service_url + PREFIX + '/' + _id
        except NameError:
            logging.error(f"Can't run the test {PREFIX}/UUID with unknown id")
            assert False

        async with aiohttp.ClientSession(headers=header) as session:
            async with session.get(url) as response:
                body = await response.json()

                assert response.status == expected_answer['status']
                assert body['title'] == expected_answer['title']
                assert len(body) == expected_answer['length']
                assert body['uuid'] == _id
                assert list(body.keys()) == ['uuid', 'title', 'imdb_rating',
                                             'description', 'genre', 'actors',
                                             'writers', 'directors']
