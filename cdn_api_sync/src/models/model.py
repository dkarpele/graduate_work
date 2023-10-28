import orjson

# Используем pydantic для упрощения работы при перегонке данных из json в
# объекты
from pydantic import BaseModel
from fastapi import Query

import core.config as conf


def orjson_dumps(v, *, default):
    # orjson.dumps возвращает bytes, а pydantic требует unicode, поэтому
    # декодируем
    return orjson.dumps(v, default=default).decode()


class Model(BaseModel):
    class Config:
        # Заменяем стандартную работу с json на более быструю
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        allow_population_by_field_name = True


class PaginateModel:
    def __init__(self,
                 page_number: int = Query(1,
                                          description=conf.PAGE_DESC,
                                          alias=conf.PAGE_ALIAS,
                                          ge=1,
                                          le=10000),
                 page_size: int = Query(50,
                                        description=conf.SIZE_DESC,
                                        alias=conf.SIZE_ALIAS,
                                        ge=1,
                                        le=500),
                 ):
        self.page_number = page_number
        self.page_size = page_size
