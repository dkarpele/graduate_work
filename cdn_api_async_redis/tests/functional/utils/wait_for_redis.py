from redis.asyncio import Redis
from logging import config as logging_config

from utils.logger import LOGGING
from settings import settings

from utils.backoff import backoff, BackoffError

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


@backoff(service='Redis')
async def wait_redis():
    redis_client = Redis(host=settings.redis_host,
                         port=settings.redis_port,
                         ssl=False)
    if not await redis_client.ping():
        raise BackoffError
