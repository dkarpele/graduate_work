import logging
from datetime import datetime
from typing import Union

from apscheduler.schedulers import SchedulerAlreadyRunningError
from fastapi import APIRouter, Request, UploadFile, HTTPException, status
from fastapi.responses import RedirectResponse

from core.config import settings
from db.aws_s3 import S3MultipartUpload, AWSS3
from db.minio_s3 import MinioS3
from db.scheduler import get_scheduler, jobs
from helpers.exceptions import object_not_exist, object_already_uploaded
from helpers.helper_async import (get_active_nodes, find_closest_node,
                                  object_exists, origin_is_alive, get_mpu_id,
                                  is_scheduler_in_progress)
from services.service import multipart_upload
from services.scheduler import copy_object_to_node
from services.mongo import MongoDep

router = APIRouter()


@router.get('/{object_name}',
            summary="Get URL to preview object (movie, music, photo)",
            response_description="Redirects to url to preview object",
            )
async def object_url(
        request: Request,
        object_name: str,
        mongo: MongoDep
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
                                  object_name,
                                  closest_node)

    endpoint = closest_node.endpoint
    access_key = closest_node.access_key_id
    secret_key = closest_node.secret_access_key

    # object doesn't exist on edge location
    if not object_ and closest_node.alias != 'origin':
        origin_node = await origin_is_alive(active_nodes)

        # Object doesn't exist on origin node too
        if not await object_exists(MinioS3,
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
        storage_data = (mongo, closest_node, object_name, "cdn")
        if not await is_scheduler_in_progress(*storage_data):
            job = await get_scheduler()
            await jobs(job,
                       copy_object_to_node,
                       args=(AWSS3, object_name, origin_node, closest_node,
                             mongo, "in_progress"),
                       next_run_time=datetime.now())
            try:
                job.start()
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

    url = await client.get_url(bucket_name=settings.bucket_name,
                               object_name=object_name)
    logging.info(f"URL created using endpoint '{endpoint}'")
    return RedirectResponse(url=url)


@router.get('/{object_name}/status',
            response_model=None,
            summary="Get status of the object uploading",
            response_description="Redirects to url to preview film",
            )
async def object_status(
        object_name: str,
        mongo: MongoDep
) -> list | HTTPException:
    query = {"object_name": object_name}
    projection = {"status": 1,
                  "node": 1}
    object_api = await mongo.get_data(query, "api", projection)
    res = []
    if object_api:
        res.append(f"'{object_name}' has status '{object_api[0]['status']}' on"
                   f" node '{object_api[0]['node']}'")

    object_cdn = await mongo.get_data(query, "cdn", projection)
    if object_cdn:
        res.append(f"'{object_name}' has status '{object_cdn[0]['status']}' on"
                   f" node '{object_cdn[0]['node']}'")

    if res:
        return res
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object {object_name} not found.",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.post('/upload_object',
             response_model=None,
             summary="Upload object to storage",
             )
async def upload_object(
        file_upload: UploadFile,
        mongo: MongoDep
) -> str | HTTPException:
    active_nodes = await get_active_nodes()
    origin_node = await origin_is_alive(active_nodes)
    filename = file_upload.filename

    await object_already_uploaded(mongo,
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

    mpu_id = await get_mpu_id(endpoint, filename, mongo)

    if await multipart_upload(mongo,
                              origin_client,
                              status="in_progress",
                              object_=file_upload,
                              mpu_id=mpu_id):
        return f"Upload {filename} completed successfully."
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Upload failed. Please retry",
            headers={"WWW-Authenticate": "Bearer"},
        )


@router.delete('/delete_object',
               response_model=None,
               summary="Delete object from all nodes",
               )
async def delete_object(
        object_name: str,
        mongo: MongoDep
) -> Union[str | HTTPException]:
    active_nodes = await get_active_nodes()

    endpoints = []
    for node in active_nodes.values():
        client = MinioS3(endpoint=node.endpoint,
                         access_key=node.access_key_id,
                         secret_key=node.secret_access_key,
                         secure=False)
        await client.remove_object(settings.bucket_name,
                                   object_name)
        endpoints.append(node.endpoint)
    document = {"object_name": object_name}
    await mongo.delete_data(document, "api")
    await mongo.delete_data(document, "cdn")
    return f"{object_name} was removed from nodes {endpoints}"
