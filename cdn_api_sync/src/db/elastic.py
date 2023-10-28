from typing import Optional
from elasticsearch import AsyncElasticsearch, NotFoundError, RequestError

from db import AbstractStorage

ES_MAX_SIZE = 50


class Elastic(AbstractStorage):
    def __init__(self, **params):
        self.session = AsyncElasticsearch(**params)

    async def get_by_id(self, _id: str, index: str, model) -> Optional:
        try:
            doc = await self.session.get(index=index, id=_id)
        except NotFoundError:
            return None
        return model(**doc['_source'])

    async def get_list(self,
                       model,
                       index: str,
                       sort: str = None,
                       search: dict = None,
                       page: int = None,
                       size: int = None) -> list | None:
        if sort:
            try:
                order = 'desc' if sort.startswith('-') else 'asc'
                sort = sort[1:] if sort.startswith('-') else sort
                sorting = [{sort: {'order': order}}]
            except AttributeError:
                sorting = None
        else:
            sorting = None

        if page and size:
            offset = (page * size) - size
        elif page and not size:
            size = ES_MAX_SIZE
            offset = (page * size) - size
        elif not page and size:
            offset = None
        else:
            offset = None
            size = ES_MAX_SIZE

        try:
            docs = await self.session.search(
                index=index,
                query=search,
                size=size,
                sort=sorting,
                from_=offset
            )
        except (NotFoundError, RequestError):
            return None

        return [model(**doc['_source']) for doc in docs['hits']['hits']]

    async def close(self):
        ...


es: Elastic | None = None


# Функция понадобится при внедрении зависимостей
async def get_elastic() -> Elastic:
    return es
