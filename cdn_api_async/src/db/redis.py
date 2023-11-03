import json

from typing import Optional
from redis.asyncio import Redis as AsyncRedis

from core.config import settings
from db import AbstractCache


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
                                    model,
                                    key: str = None,
                                    sort: str = None) -> list | None:
        data = await self.session.hgetall(key)
        if not data:
            return None

        res = data.values()
        if sort:
            if sort[0] == '-':
                # Раз в сортировке есть знак минус, то его нужно убрать,
                # чтобы получить название поля, по которому идёт сортировка.
                # Используем срез, убирая нулевой элемент
                res = sorted(res, key=lambda x: json.loads(x)[sort[1:]],
                             reverse=True)
            else:
                res = sorted(res, key=lambda x: json.loads(x)[sort])
        return [model.parse_raw(i) for i in res]

    async def put_to_cache_by_key(self,
                                  key: str = None,
                                  entities: list = None):
        entities_dict: dict = \
            {item: entity.json() for item, entity in enumerate(entities)}
        await self.session.hset(name=key,
                                mapping=entities_dict)
        await self.session.expire(name=key,
                                  time=settings.CACHE_EXPIRE_IN_SECONDS)

    async def create_pipeline(self):
        return self.session.pipeline()


redis: Redis | None = None


# Функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis
