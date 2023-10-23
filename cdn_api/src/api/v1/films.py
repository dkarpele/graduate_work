import logging

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from core.config import settings
from db.minio_s3 import MinioS3
from helpers.exceptions import object_not_exist
from helpers.helper import get_active_nodes, find_closest_node, object_exists, \
    origin_is_alive

router = APIRouter()


@router.get('/{film_title}',
            summary="URL to preview film",
            response_description="url",
            )
async def film_url(
        request: Request,
        film_title: str
) -> RedirectResponse:
    client_host = request.client.host
    client_host = "137.0.0.1"
    active_nodes = await get_active_nodes()
    closest_node = await find_closest_node(client_host,
                                           active_nodes)
    if not closest_node and await origin_is_alive(active_nodes):
        closest_node = active_nodes['ORIGIN']
        logging.info(f"Use Origin S3 '{closest_node.alias}'")

    # Check if object exists in the closest edge location
    object_ = await object_exists(MinioS3,
                                  settings.bucket_name,
                                  film_title,
                                  closest_node)

    endpoint = closest_node.endpoint
    access_key = closest_node.access_key_id
    secret_key = closest_node.secret_access_key

    # object doesn't exist on edge location
    if not object_ and closest_node.alias != 'local':
        # Use endpoint and creds from origin to create url
        origin_node = await origin_is_alive(active_nodes)
        endpoint = origin_node.endpoint
        access_key = origin_node.access_key_id
        secret_key = origin_node.secret_access_key
        # Copy object to closest_node using Scheduler
    # object doesn't exist on origin location
    elif not object_ and closest_node.alias == 'local':
        # Nothing to be copied. Raise exception
        raise object_not_exist(film_title, settings.bucket_name)

    client = MinioS3(endpoint=endpoint,
                     access_key=access_key,
                     secret_key=secret_key,
                     secure=False)

    url = await client.get_url(bucket_name=settings.bucket_name,
                               object_name=film_title)

    return RedirectResponse(url=url)
