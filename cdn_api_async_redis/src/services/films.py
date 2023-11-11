import logging
from datetime import datetime

from apscheduler.schedulers import SchedulerAlreadyRunningError
from fastapi import HTTPException
from starlette import status

from connectors.aws_s3 import AWSS3, S3MultipartUpload
from connectors.minio_s3 import MinioS3
from connectors.scheduler import jobs, copy_object_to_node
from core.config import settings
from helpers.exceptions import object_not_exist, object_already_uploaded
from helpers.helper_async import get_object, origin_is_alive, \
    is_scheduler_in_progress
from models.model import Status


async def get_client_data(active_nodes, cache, closest_node, object_name,
                          scheduler):
    # Check if object exists in the closest edge location
    object_ = await get_object(MinioS3,
                               settings.bucket_name,
                               object_name,
                               closest_node)
    endpoint = closest_node.endpoint
    access_key = closest_node.access_key_id
    secret_key = closest_node.secret_access_key
    # object doesn't exist on edge location
    if not object_ and closest_node.alias != 'origin':
        origin_node = await origin_is_alive(active_nodes)

        # Object doesn't exist on origin node too
        if not await get_object(MinioS3,
                                settings.bucket_name,
                                object_name,
                                origin_node):
            # Nothing to be copied. Raise exception
            raise await object_not_exist(object_name,
                                         settings.bucket_name)

        # Use endpoint and creds from origin to create url
        endpoint = origin_node.endpoint
        access_key = origin_node.access_key_id
        secret_key = origin_node.secret_access_key

        # Copy object to closest_node using Scheduler
        storage_data = (cache, closest_node, object_name, "cdn")
        if not await is_scheduler_in_progress(*storage_data):
            await jobs(scheduler,
                       copy_object_to_node,
                       args=(AWSS3, object_name, origin_node, closest_node,
                             cache, Status.IN_PROGRESS.value),
                       next_run_time=datetime.now())
            try:
                scheduler.start()
            except SchedulerAlreadyRunningError as e:
                logging.info(e)

    # object doesn't exist on origin location
    elif not object_ and closest_node.alias == 'origin':
        # Nothing to be copied. Raise exception
        raise await object_not_exist(object_name, settings.bucket_name)
    client = MinioS3(endpoint=endpoint,
                     access_key=access_key,
                     secret_key=secret_key,
                     secure=False)
    return client, endpoint


async def get_multipart_upload_client_data(cache, file_upload, filename,
                                           origin_node):
    await object_already_uploaded(cache,
                                  origin_node,
                                  filename,
                                  collection="api")
    endpoint = 'http://' + origin_node.endpoint
    origin_client_dict = {
        'endpoint': endpoint,
        'aws_access_key_id': origin_node.access_key_id,
        'aws_secret_access_key': origin_node.secret_access_key,
        'verify': False}
    origin_client: S3MultipartUpload = S3MultipartUpload(
        settings.bucket_name,
        filename,
        content_type=file_upload.content_type,
        total_bytes=file_upload.size,
        **origin_client_dict)
    return endpoint, origin_client


async def process_deleting_object(active_nodes, cache, object_name):
    endpoints = []
    for node in active_nodes.values():
        object_ = await get_object(MinioS3,
                                   settings.bucket_name,
                                   object_name,
                                   node)
        if not object_:
            logging.info(f"{object_name} doesn't exist on {node.endpoint}")
            continue
        client = MinioS3(endpoint=node.endpoint,
                         access_key=node.access_key_id,
                         secret_key=node.secret_access_key,
                         secure=False)
        await client.remove_object(settings.bucket_name,
                                   object_name)
        endpoint = 'http://' + node.endpoint
        endpoints.append(endpoint)
        key_api = f"api^{object_name}^{endpoint}"
        key_cdn = f"cdn^{object_name}^{endpoint}"
        await cache.delete_from_cache_by_id(key_api)
        await cache.delete_from_cache_by_id(key_cdn)
    if not endpoints:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{object_name} doesn't exist on all nodes!",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return endpoints
