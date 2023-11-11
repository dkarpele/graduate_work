from datetime import datetime

from starlette.requests import Request

from core.config import rl
from dependencies.redis import CacheDep
from helpers.exceptions import too_many_requests
from services.backoff import backoff


@backoff(service='Redis')
async def rate_limit(request: Request,
                     cache: CacheDep):
    """
    Ограничение количества запросов к серверу (Rate limit).
    Используется алгоритм Leaky bucket. Запросы, которые не прошли лимит,
    получат HTTP-статус 429 Too Many Requests.
    :param request: Из request получим хост:порт юзера
    :param cache: redis cache
    :return:
    """
    if not rl.is_rate_limit:
        return
    pipe = await cache.get_pipeline()
    now = datetime.now()
    host = str(request.client)
    key = f'{host}:{now.minute}'
    pipe.incr(key, 1)
    pipe.expire(key, 59)
    result = await pipe.execute()
    request_number = result[0]
    if request_number > rl.request_limit_per_minute:
        raise too_many_requests
