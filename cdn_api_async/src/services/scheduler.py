import logging
import os
from datetime import datetime
from typing import Type

import magic
from aiofiles import os
from aioshutil import rmtree
from core.config import settings
from db import AbstractS3, AbstractStorage
from db.aws_s3 import S3MultipartUpload
from helpers.exceptions import object_already_uploaded
from helpers.helper_async import get_mpu_id
from services.service import multipart_upload
from models.model import Node


async def copy_object_file_to_node(client: Type[AbstractS3],
                                   object_name: str,
                                   origin_node: Node,
                                   edge_node: Node) -> None:
    result = False
    origin_client: AbstractS3 = client(
        endpoint='http://' + origin_node.endpoint,
        aws_access_key_id=origin_node.access_key_id,
        aws_secret_access_key=origin_node.secret_access_key,
        verify=False)

    dir_name = f'/tmp/{datetime.now().isoformat()}'
    try:
        await os.makedirs(dir_name, exist_ok=True)
        file_path = f'{dir_name}/{object_name}'
        await origin_client.fget_object(
            settings.bucket_name,
            object_name,
            file_path)

        edge_client_dict = {
            'endpoint': 'http://' + edge_node.endpoint,
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
        async with edge_client.client as s3:
            await edge_client.abort_all(s3)
            # create new multipart upload
            mpu_id = await edge_client.create(s3)
            logging.info(f"Starting upload with id={mpu_id}")
            # upload parts
            parts = await edge_client.upload_file(s3, mpu_id)
            result = await edge_client.complete(s3, mpu_id, parts)
            logging.info(result)

    finally:
        if result:
            await rmtree(dir_name, ignore_errors=True)
        logging.info(f"Removing temp dir {dir_name}")


async def copy_object_to_node(client: Type[AbstractS3],
                              object_name: str,
                              origin_node: Node,
                              edge_node: Node,
                              storage: AbstractStorage) -> None:
    await object_already_uploaded(storage,
                                  edge_node,
                                  object_name,
                                  collection="cdn")

    origin_client: AbstractS3 = client(
        endpoint='http://' + origin_node.endpoint,
        aws_access_key_id=origin_node.access_key_id,
        aws_secret_access_key=origin_node.secret_access_key,
        verify=False)
    async with origin_client.client as s3:
        obj = await origin_client.get_object(
            settings.bucket_name,
            object_name,
            s3=s3)

        endpoint = 'http://' + edge_node.endpoint
        edge_client_dict = {
            'endpoint': endpoint,
            'aws_access_key_id': edge_node.access_key_id,
            'aws_secret_access_key': edge_node.secret_access_key,
            'verify': False}
        total_bytes = int(
            obj['ContentRange'][obj['ContentRange'].rindex('/')+1:])
        edge_client = S3MultipartUpload(settings.bucket_name,
                                        object_name,
                                        total_bytes=total_bytes,
                                        content_type=obj['ContentType'],
                                        **edge_client_dict)

        # If upload was failed - try to re-upload it with current mpu_id
        mpu_id = await get_mpu_id(endpoint,
                                  object_name,
                                  storage,
                                  collection="cdn")

        logging.info(
            f"Uploading '{object_name}' from '{origin_client.endpoint}' to "
            f"'{edge_client.endpoint}'.")
        await multipart_upload(storage=storage,
                               upload_client=edge_client,
                               origin_client=origin_client,
                               origin_client_s3=s3,
                               collection="cdn",
                               mpu_id=mpu_id)
