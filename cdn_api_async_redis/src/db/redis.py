from typing import Optional

from redis.asyncio import Redis as AsyncRedis

from db.abstract import AbstractCache


class Redis(AbstractCache):
    def __init__(self, **params):
        self.session = AsyncRedis(**params)

    async def close(self):
        ...

    async def get_from_cache_by_id(self, _id: str) -> Optional:
        data = await self.session.get(_id)
        if not data:
            return None

        return data

    async def put_to_cache_by_id(self, _id, entity, expire):
        await self.session.set(_id,
                               entity,
                               expire)

    async def delete_from_cache_by_id(self, _id):
        await self.session.delete(_id)

    async def get_from_cache_by_key(self,
                                    key: str = None,
                                    sort: str = None) -> dict | None:
        data = await self.session.hgetall(key)
        if not data:
            return None

        return data

    async def put_to_cache_by_key(self,
                                  key: str = None,
                                  entities: dict = None):

        await self.session.hset(name=key,
                                mapping=entities)
        # await self.session.expire(name=key,
        #                           time=settings.cache_expire_in_seconds)

    async def create_pipeline(self):
        return self.session.pipeline()

    async def get_keys_by_pattern(self,
                                  pattern: str = None,):
        data = self.session.scan_iter(pattern)
        return data


redis: Redis | None = None


# Функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis
