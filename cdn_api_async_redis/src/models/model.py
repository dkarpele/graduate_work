from datetime import datetime
from enum import Enum
from dataclasses import dataclass

import orjson
from pydantic import BaseModel, Field


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


class Status(Enum):
    FINISHED = 'finished'
    IN_PROGRESS = 'in_progress'
    SCHEDULER_IN_PROGRESS = 'scheduler_in_progress'
