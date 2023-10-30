from typing import Optional

from db import AbstractStorage, AbstractCache, AbstractS3


class CDNService:
    def __init__(self, s3: AbstractS3):
        self.s3 = s3

    def download(self, bucket_name, object_name):
        self.s3.get_url(bucket_name=bucket_name,
                        object_name=object_name)


class IdRequestService:
    def __init__(self, cache: AbstractCache, storage: AbstractStorage, model):
        self.cache = cache
        self.storage = storage
        self.model = model

    async def process_by_id(self, _id: str, index: str) -> Optional:
        entity = await self.cache.get_from_cache_by_id(_id=_id,
                                                       model=self.model)
        if not entity:
            entity = await self.storage.get_by_id(_id, index, self.model)
            if not entity:
                return None
            await self.cache.put_to_cache_by_id(entity=entity)

        return entity


class ListService:
    def __init__(self, cache: AbstractCache, storage: AbstractStorage, model):
        self.cache = cache
        self.storage = storage
        self.model = model

    async def process_list(self,
                           index: str,
                           sort: str = None,
                           search: dict = None,
                           key: str = None,
                           page: int = None,
                           size: int = None) -> Optional:

        if key:
            entities = await self.cache.get_from_cache_by_key(model=self.model,
                                                              key=key,
                                                              sort=sort)
        else:
            entities = None

        if not entities:
            entities = await self.storage.get_list(self.model,
                                                   index,
                                                   sort,
                                                   search,
                                                   page,
                                                   size)
            if not entities:
                return None
            if key:
                await self.cache.put_to_cache_by_key(key, entities)

        return entities
