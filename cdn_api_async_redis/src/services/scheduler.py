import logging
from datetime import datetime, timedelta
from typing import Type

import magic
from aiofiles import os
from aioshutil import rmtree

from core.config import settings
from db import AbstractS3, AbstractCache
from db.aws_s3 import S3MultipartUpload, AWSS3
from helpers.exceptions import object_already_uploaded
from helpers.helper_async import get_mpu_id, get_active_nodes, origin_is_alive
from models.model import Node
from services.service import multipart_upload


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
                              cache: AbstractCache,
                              status: str) -> None:
    # Check that object wasn't uploaded yet and not blocked by scheduler
    storage_data = (cache, edge_node, object_name, "cdn")
    await object_already_uploaded(*storage_data)

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
            obj['ContentRange'][obj['ContentRange'].rindex('/') + 1:])
        edge_client = S3MultipartUpload(settings.bucket_name,
                                        object_name,
                                        total_bytes=total_bytes,
                                        content_type=obj['ContentType'],
                                        **edge_client_dict)

        # If upload was failed - try to re-upload it with current mpu_id
        mpu_id = await get_mpu_id(endpoint,
                                  object_name,
                                  cache,
                                  collection="cdn")

        logging.info(
            f"Uploading '{object_name}' from '{origin_client.endpoint}' to "
            f"'{edge_client.endpoint}'.")
        await multipart_upload(cache=cache,
                               upload_client=edge_client,
                               origin_client=origin_client,
                               origin_client_s3=s3,
                               status=status,
                               collection="cdn",
                               mpu_id=mpu_id,
                               )


async def finish_in_progress_tasks(client: Type[AWSS3],
                                   cache: AbstractCache, ) -> None:
    key_pattern = "cdn^*^"
    await get_cached_values(cache,
                            client,
                            key_pattern,
                            True)


async def abort_old_tasks(client: Type[S3MultipartUpload],
                          cache: AbstractCache, ):
    key_pattern = "^*^"
    await get_cached_values(cache,
                            client,
                            key_pattern,
                            finish=False)


async def get_cached_values(cache: AbstractCache,
                            client: Type[AWSS3],
                            key_pattern: str,
                            finish: bool = True):
    time_now = datetime.utcnow() - timedelta(hours=6)
    active_nodes = await get_active_nodes()
    origin_node = await origin_is_alive(active_nodes)

    for node in active_nodes.values():
        endpoint = 'http://' + node.endpoint
        key_pattern += endpoint
        async for key in await cache.scan_iter(key_pattern):
            if not key:
                logging.info(
                    f"No in progress objects from {time_now} for "
                    f"{endpoint}. Everything good.")
            else:
                obj = await cache.get_from_cache_by_key(key)
                key = str(key, 'utf-8')
                object_name = key[key.index('^') + 1:key.rindex('^')]
                last_modified = datetime.strptime(str(obj[b'last_modified'],
                                                      'utf-8'),
                                                  '%Y-%m-%d %H:%M:%S.%f')
                comparing = last_modified > time_now \
                    if finish \
                    else last_modified < time_now
                if str(obj[b'status'], 'utf-8') in ('in_progress',
                                                    'scheduler_in_progress') \
                        and comparing:
                    if finish:
                        await copy_object_to_node(client,
                                                  object_name,
                                                  origin_node,
                                                  node,
                                                  cache,
                                                  "scheduler_in_progress")
                    else:
                        client_dict = {
                            'endpoint': endpoint,
                            'aws_access_key_id': node.access_key_id,
                            'aws_secret_access_key': node.secret_access_key,
                            'verify': False}
                        client = S3MultipartUpload(settings.bucket_name,
                                                   object_name,
                                                   **client_dict)
                        async with client.client as s3:
                            mpu_id = str(obj[b'mpu_id'], 'utf-8')
                            await client.abort_multipart_upload(s3,
                                                                mpu_id)
                            await cache.delete_from_cache_by_id(key)
