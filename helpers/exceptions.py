import logging

from fastapi import HTTPException, status

from db import AbstractStorage
from models.model import Node

credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)

relogin_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Your credentials expired. Please login again.",
    headers={"WWW-Authenticate": "Bearer"},
)

access_token_invalid_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Access token expired. Create new token with /refresh",
    headers={"WWW-Authenticate": "Bearer"},
)

wrong_username_or_password = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Incorrect username or password",
    headers={"WWW-Authenticate": "Bearer"},
)


def entity_doesnt_exist(err: Exception) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"{err}",
        headers={"WWW-Authenticate": "Bearer"},
    )


def object_already_uploaded(storage: AbstractStorage,
                            node: Node,
                            object_: str,
                            collection: str) -> None:
    endpoint = 'http://' + node.endpoint
    query = {'object_name': object_,
             'node': endpoint,
             'status': 'finished'}
    projection = {'object_name': 1}
    res = storage.get_data(query=query,
                           collection=collection,
                           projection=projection
                           )
    if res:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{object_} was already successfully uploaded to"
                   f" {endpoint}. If you want to upload an object with the "
                   f"same name, you need to remove the old one first.",
            headers={"WWW-Authenticate": "Bearer"},
        )


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


def object_not_exist(object_name: str, bucket_name: str) -> HTTPException:
    message = f"'{object_name}' doesn't exist in '{bucket_name}' bucket"
    logging.error(message)
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=message,
        headers={"WWW-Authenticate": "Bearer"},
    )
