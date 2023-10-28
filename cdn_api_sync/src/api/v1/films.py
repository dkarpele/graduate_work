import logging
from datetime import datetime
from typing import Union

from apscheduler.schedulers import SchedulerAlreadyRunningError
from fastapi import APIRouter, Request, UploadFile, HTTPException, status
from fastapi.responses import RedirectResponse

from core.config import settings
from db.aws_s3 import AWSS3, S3MultipartUpload
from db.minio_s3 import MinioS3
from db.scheduler import get_scheduler, jobs
from helpers.exceptions import object_not_exist
from helpers.helper_sync import (get_active_nodes, find_closest_node,
                                 object_exists, origin_is_alive,
                                 copy_object_file_to_node, multipart_upload,
                                 copy_object_to_node)

router = APIRouter()


@router.get('/{film_title}',
            summary="Get URL to preview film",
            response_description="Redirects to url to preview film",
            )
def film_url(
        request: Request,
        film_title: str
) -> RedirectResponse:
    client_host = request.client.host
    client_host = "137.0.0.1"
    active_nodes = get_active_nodes()
    closest_node = find_closest_node(client_host,
                                     active_nodes)
    if not closest_node and origin_is_alive(active_nodes):
        closest_node = active_nodes['ORIGIN']
        logging.info(f"Use Origin S3 '{closest_node.alias}'")

    # Check if object exists in the closest edge location
    object_ = object_exists(MinioS3,
                            settings.bucket_name,
                            film_title,
                            closest_node)

    endpoint = closest_node.endpoint
    access_key = closest_node.access_key_id
    secret_key = closest_node.secret_access_key

    # object doesn't exist on edge location
    if not object_ and closest_node.alias != 'origin':
        # Use endpoint and creds from origin to create url
        origin_node = origin_is_alive(active_nodes)
        endpoint = origin_node.endpoint
        access_key = origin_node.access_key_id
        secret_key = origin_node.secret_access_key

        # Copy object to closest_node using Scheduler
        job = get_scheduler()
        jobs(job,
             copy_object_to_node,
             args=(MinioS3, film_title, origin_node, closest_node),
             next_run_time=datetime.now())
        try:
            job.start()
        except SchedulerAlreadyRunningError as e:
            logging.info(e)

    # object doesn't exist on origin location
    elif not object_ and closest_node.alias == 'origin':
        # Nothing to be copied. Raise exception
        raise object_not_exist(film_title, settings.bucket_name)

    client = MinioS3(endpoint=endpoint,
                     access_key=access_key,
                     secret_key=secret_key,
                     secure=False)

    url = client.get_url(bucket_name=settings.bucket_name,
                         object_name=film_title)
    logging.info(f"URL created using endpoint '{endpoint}'")
    return RedirectResponse(url=url)


@router.post('/upload_object',
             response_model=None,
             summary="Upload object to storage",
             )
def upload_object(
        file_upload: UploadFile
) -> Union[str | HTTPException]:
    active_nodes = get_active_nodes()
    origin_node = origin_is_alive(active_nodes)

    origin_client_dict = {
        'endpoint_url': 'http://' + origin_node.endpoint,
        'aws_access_key_id': origin_node.access_key_id,
        'aws_secret_access_key': origin_node.secret_access_key,
        'verify': False}

    origin_client: S3MultipartUpload = S3MultipartUpload(
        settings.bucket_name,
        file_upload.filename,
        content_type=file_upload.content_type,
        total_bytes=file_upload.size,
        **origin_client_dict)

    if multipart_upload(origin_client, file_upload.file, is_api=True):
        return "Upload completed successfully."
    else:
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Upload failed. Please retry",
            headers={"WWW-Authenticate": "Bearer"},
        )
