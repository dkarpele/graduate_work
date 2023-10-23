import pytest_asyncio

from elasticsearch.helpers import async_bulk

from tests.functional.settings import settings
from tests.functional.testdata import es_data, es_schemas


@pytest_asyncio.fixture(scope='class')
async def es_write_data(es_client):
    def get_es_bulk_query(_index, data):
        doc = []
        for row in data:
            doc.append(
                {'_index': _index,
                 '_id': row[settings.es_id_field],
                 '_source': row
                 })
        return doc

    for index in settings.es_indexes.split():
        schema = es_schemas.schemas[index]
        await es_client.indices.create(index=index,
                                       settings=schema['settings'],
                                       mappings=schema['mappings'])

        bulk_query = get_es_bulk_query(index, es_data.data[index])
        await async_bulk(es_client, bulk_query)
        await es_client.indices.refresh()

    yield

    for index in settings.es_indexes.split():
        await es_client.indices.flush(index=index)
        await es_client.indices.delete(index=index)
