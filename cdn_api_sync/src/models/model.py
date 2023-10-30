from dataclasses import dataclass

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


@dataclass
class Node:
    endpoint: str
    alias: str
    access_key_id: str
    secret_access_key: str
    city: str
    latitude: float
    longitude: float
    is_active: str
