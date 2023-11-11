import logging

from fastapi import HTTPException, status

from connectors.abstract import AbstractCache
from models.model import Node, Status


async def object_already_uploaded(cache: AbstractCache,
                                  node: Node,
                                  object_: str,
                                  collection: str) -> None:
    endpoint = 'http://' + node.endpoint
    key: str = f"{collection}^{object_}^{endpoint}"

    res = await cache.get_from_cache_by_key(key)
    try:
        if str(res[b'status'], 'utf-8') == Status.FINISHED.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{object_} was already successfully uploaded to"
                       f" {endpoint}. If you want to upload an object with the"
                       f" same name, you need to remove the old one first.",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except TypeError:
        return None


too_many_requests = HTTPException(
    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
    detail="Too many requests",
    headers={"WWW-Authenticate": "Bearer"},
)

locations_not_available = HTTPException(
    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    detail="All S3 locations are not available",
    headers={"WWW-Authenticate": "Bearer"},
)


async def object_not_exist(object_name: str,
                           bucket_name: str) -> HTTPException:
    message = f"'{object_name}' doesn't exist in '{bucket_name}' bucket"
    logging.error(message)
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=message,
        headers={"WWW-Authenticate": "Bearer"},
    )
