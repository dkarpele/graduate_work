from functools import lru_cache
from typing import Annotated

from connectors.redis import Redis, get_redis
from connectors.abstract import AbstractCache
from fastapi import Depends


@lru_cache()
def get_cache_service(
        redis: Redis = Depends(get_redis)) -> AbstractCache:
    return redis


CacheDep = Annotated[AbstractCache, Depends(get_cache_service)]
