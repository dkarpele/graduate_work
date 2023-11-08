from elasticsearch import Elasticsearch
from logging import config as logging_config

from utils.logger import LOGGING
from settings import settings

from utils.backoff import backoff, BackoffError

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


@backoff(service='ES')
async def wait_es():
    es_client = Elasticsearch(
        hosts=f'http://{settings.elastic_host}:{settings.elastic_port}',
        validate_cert=False,
        use_ssl=False)
    if not es_client.ping():
        raise BackoffError()
