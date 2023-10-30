import logging
import os
from datetime import datetime
from shutil import rmtree
from typing import Type

import magic

from core.config import settings
from db import AbstractS3, AbstractStorage
from db.aws_s3 import S3MultipartUpload
from helpers.exceptions import object_already_uploaded
from helpers.helper_sync import get_mpu_id
from services.service import multipart_upload
from models.model import Node


def copy_object_file_to_node(client: Type[AbstractS3],
                             object_name: str,
                             origin_node: Node,
                             edge_node: Node) -> None:
    result = False
    origin_client: AbstractS3 = client(
        endpoint_url='http://' + origin_node.endpoint,
        aws_access_key_id=origin_node.access_key_id,
        aws_secret_access_key=origin_node.secret_access_key,
        verify=False)

    dir_name = f'/tmp/{datetime.now().isoformat()}'
    try:
        os.makedirs(dir_name, exist_ok=True)
        file_path = f'{dir_name}/{object_name}'
        origin_client.fget_object(
            settings.bucket_name,
            object_name,
            file_path)

        edge_client_dict = {
            'endpoint_url': 'http://' + edge_node.endpoint,
            'aws_access_key_id': edge_node.access_key_id,
            'aws_secret_access_key': edge_node.secret_access_key,
            'verify': False}
        content_type = magic.from_file(file_path,
                                       mime=True)
        edge_client = S3MultipartUpload(settings.bucket_name,
                                        object_name,
                                        file_path,
                                        content_type,
                                        **edge_client_dict)

        edge_client.abort_all()
        # create new multipart upload
        mpu_id = edge_client.create()
        logging.info(f"Starting upload with id={mpu_id}")
        # upload parts
        parts = edge_client.upload_file(mpu_id)
        result = edge_client.complete(mpu_id, parts)
        logging.info(result)

    finally:
        if result:
            rmtree(dir_name, ignore_errors=True)
        logging.info(f"Removing temp dir {dir_name}")


def copy_object_to_node(client: Type[AbstractS3],
                        object_name: str,
                        origin_node: Node,
                        edge_node: Node,
                        storage: AbstractStorage) -> None:
    object_already_uploaded(storage,
                            edge_node,
                            object_name,
                            collection="cdn")

    origin_client: AbstractS3 = client(
        endpoint=origin_node.endpoint,
        access_key=origin_node.access_key_id,
        secret_key=origin_node.secret_access_key,
        secure=False)

    obj = origin_client.stat_object(
        settings.bucket_name,
        object_name)

    endpoint = 'http://' + edge_node.endpoint
    edge_client_dict = {
        'endpoint_url': endpoint,
        'aws_access_key_id': edge_node.access_key_id,
        'aws_secret_access_key': edge_node.secret_access_key,
        'verify': False}

    edge_client = S3MultipartUpload(settings.bucket_name,
                                    object_name,
                                    total_bytes=obj.size,
                                    content_type=obj.content_type,
                                    **edge_client_dict)

    # If upload was failed - try to re-upload it with current mpu_id
    mpu_id = get_mpu_id(endpoint, object_name, storage, collection="cdn")

    logging.info(f"Uploading '{object_name}' from '{origin_node.endpoint}' to "
                 f"'{edge_node.endpoint}'.")
    multipart_upload(storage=storage,
                     upload_client=edge_client,
                     origin_client=origin_client,
                     collection="cdn",
                     mpu_id=mpu_id)
