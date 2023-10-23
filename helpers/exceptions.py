import logging

from fastapi import HTTPException, status

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


def entity_doesnt_exist(name: str, value: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=f"{name} {value} doesn't exist",
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
