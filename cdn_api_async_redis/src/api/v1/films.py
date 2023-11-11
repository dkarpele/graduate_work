import logging

from fastapi import APIRouter, Request, UploadFile, HTTPException, status
from fastapi.responses import RedirectResponse

from core.config import settings
from helpers.helper_async import (get_active_nodes, find_closest_node,
                                  origin_is_alive, get_mpu_id)
from models.model import Status
from dependencies.redis import CacheDep
from dependencies.scheduler import SchedulerDep
from services.films import get_client_data, get_multipart_upload_client_data, \
    process_deleting_object
from services.service import multipart_upload

router = APIRouter()


@router.get('/{object_name}',
            summary="Get URL to preview object (movie, music, photo)",
            response_description="Redirects to url to preview object",
            )
async def object_url(
        request: Request,
        object_name: str,
        cache: CacheDep,
        scheduler: SchedulerDep
) -> RedirectResponse:
    client_host = request.client.host
    # Stub to test CDN on localhost
    # client_host = "137.0.0.1"
    active_nodes = await get_active_nodes()
    closest_node = await find_closest_node(client_host,
                                           active_nodes)
    if not closest_node and await origin_is_alive(active_nodes):
        closest_node = active_nodes['ORIGIN']
        logging.info(f"Use Origin S3 '{closest_node.alias}'")

    client, endpoint = await get_client_data(active_nodes,
                                             cache,
                                             closest_node,
                                             object_name,
                                             scheduler)

    url = await client.get_url(bucket_name=settings.bucket_name,
                               object_name=object_name)
    logging.info(f"URL created using endpoint '{endpoint}'")
    return RedirectResponse(url=url)


@router.get('/{object_name}/status',
            response_model=None,
            summary="Get status of the object uploading",
            )
async def object_status(
        object_name: str,
        cache: CacheDep
) -> str | HTTPException:
    active_nodes = await get_active_nodes()
    origin_node = await origin_is_alive(active_nodes)
    endpoint = 'http://' + origin_node.endpoint
    key: str = f"api^{object_name}^{endpoint}"

    object_api = await cache.get_from_cache_by_key(key)

    if object_api:
        return (f"'{object_name}' has status "
                f"'{str(object_api[b'status'], 'utf-8')}' "
                f"on node '{endpoint}'")
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object {object_name} not found.",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.post('/object',
             response_model=None,
             summary="Upload object to storage",
             )
async def upload_object(
        file_upload: UploadFile,
        cache: CacheDep
) -> str | HTTPException:
    active_nodes = await get_active_nodes()
    origin_node = await origin_is_alive(active_nodes)
    filename = file_upload.filename

    endpoint, origin_client = await get_multipart_upload_client_data(
        cache,
        file_upload,
        filename,
        origin_node)

    mpu_id = await get_mpu_id(endpoint, filename, cache)

    if await multipart_upload(cache,
                              origin_client,
                              status=Status.IN_PROGRESS.value,
                              object_=file_upload,
                              mpu_id=mpu_id):
        return f"Upload {filename} completed successfully."
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Upload failed. Please retry",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.delete('/object',
               response_model=None,
               summary="Delete object from all nodes",
               )
async def delete_object(
        object_name: str,
        cache: CacheDep
) -> str | HTTPException:
    active_nodes = await get_active_nodes()

    endpoints = await process_deleting_object(active_nodes,
                                              cache,
                                              object_name)
    return f"{object_name} was removed from nodes {endpoints}"
