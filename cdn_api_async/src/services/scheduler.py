import logging
from datetime import datetime, timedelta
from typing import Type

import magic
from aiofiles import os
from aioshutil import rmtree

from core.config import settings
from db import AbstractS3, AbstractStorage
from db.aws_s3 import S3MultipartUpload
from helpers.exceptions import object_already_uploaded
from helpers.helper_async import get_mpu_id, get_in_progress_objects, \
    origin_is_alive, get_active_nodes
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
                              storage: AbstractStorage,
                              status: str) -> None:
    # Check that object wasn't uploaded yet and not blocked by scheduler
    storage_data = (storage, edge_node, object_name, "cdn")
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
                                  storage,
                                  collection="cdn")

        logging.info(
            f"Uploading '{object_name}' from '{origin_client.endpoint}' to "
            f"'{edge_client.endpoint}'.")
        await multipart_upload(storage=storage,
                               upload_client=edge_client,
                               origin_client=origin_client,
                               origin_client_s3=s3,
                               status=status,
                               collection="cdn",
                               mpu_id=mpu_id,
                               )


async def finish_in_progress_tasks(client: Type[AbstractS3],
                                   storage: AbstractStorage, ) -> None:
    time_ = datetime.utcnow() - timedelta(hours=6)
    threshold = {
        '$gt': time_
    }
    active_nodes = await get_active_nodes()
    origin_node = await origin_is_alive(active_nodes)

    objects_: list = await get_in_progress_objects(storage,
                                                   "cdn",
                                                   threshold)
    if not objects_:
        logging.info(f"No in progress objects from {time_}. Everything good.")
        return

    for i in objects_:
        # Trying to find Node object
        edge_node = [j
                     for j in active_nodes.values()
                     if j.endpoint in i['node']
                     ]
        if edge_node:
            await copy_object_to_node(client,
                                      i['object_name'],
                                      origin_node,
                                      edge_node[0],
                                      storage,
                                      "scheduler_in_progress")
        else:
            logging.info(f"No in progress objects from {time_}. "
                         f"Everything good.")


async def abort_old_tasks(client: Type[S3MultipartUpload],
                          storage: AbstractStorage, ):
    time_ = datetime.utcnow() - timedelta(hours=6)
    threshold = {
        '$lt': time_
    }
    active_nodes = await get_active_nodes()

    objects_cdn: list = await get_in_progress_objects(storage,
                                                      "cdn",
                                                      threshold)
    objects_api: list = await get_in_progress_objects(storage,
                                                      "api",
                                                      threshold)
    objects_ = objects_api + objects_cdn
    if not objects_:
        logging.info(f"No in progress objects older than {time_}. "
                     f"Everything good.")
        return

    for i in objects_:
        # Trying to find Node object
        edge_node = [j
                     for j in active_nodes.values()
                     if j.endpoint in i['node']
                     ]
        if edge_node:
            en = edge_node[0]
            endpoint = 'http://' + en.endpoint
            edge_client_dict = {
                'endpoint': endpoint,
                'aws_access_key_id': en.access_key_id,
                'aws_secret_access_key': en.secret_access_key,
                'verify': False}
            edge_client = client(settings.bucket_name,
                                 i['object_name'],
                                 **edge_client_dict)
            async with edge_client.client as s3:
                await edge_client.abort_all(s3)

            # clear storage
            document = {"object_name": i['object_name']}
            await storage.delete_data(document, "api")
            await storage.delete_data(document, "cdn")
        else:
            logging.info(f"No in progress objects older than {time_}. "
                         f"Everything good.")
