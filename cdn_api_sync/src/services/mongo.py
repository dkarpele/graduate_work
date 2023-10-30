from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from db.mongo import get_mongo, Mongo


@lru_cache()
def get_mongo_service(
        mongo: Mongo = Depends(get_mongo)) -> Mongo:
    return mongo


MongoDep = Annotated[Mongo, Depends(get_mongo_service)]
