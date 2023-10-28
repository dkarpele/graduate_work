import logging
import os
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError, loads
from shutil import rmtree
from typing import Any, Type

import magic
import requests
from aiohttp import ClientConnectorError
from dotenv import load_dotenv
from geopy.distance import distance

from core.config import settings
from db import AbstractS3
from db.aws_s3 import S3MultipartUpload
from helpers.exceptions import locations_not_available

load_dotenv()


@dataclass
class Node:
    endpoint: str
    alias: str
    access_key_id: str
    secret_access_key: str
    city: str
    latitude: float
    longitude: float
    is_active: str


def get_active_nodes(
        file_path: str = "./.env.minio.json"
) -> dict[str, Node]:
    """
    Get origin and all active edge locations (is_active = True)
    :param file_path: Edge and origin configuration
    :return: Collection of {node name: Node}
    """
    try:
        with open(file_path, "r") as file:
            j = loads(file.read())
            return {k: Node(**v) for k, v in j.items()
                    if v['is_active'] == "True"}

    except FileNotFoundError:
        logging.error('File with nodes doesn\'t exist!!!')
        raise FileNotFoundError
    except JSONDecodeError as e:
        logging.error(e)
    except AttributeError:
        logging.error('Attribute Error!!!')
    except IOError:
        logging.error('IOError!!!')


def origin_is_alive(nodes: dict[str, Node]):
    try:
        return nodes['ORIGIN']
    except KeyError:
        logging.error("ORIGIN node died. Failing")
        raise locations_not_available


def find_closest_node(user_ip: str,
                      active_nodes: dict[str, Node]) -> Node | bool:
    """
    Get node that has the shortest distance between user's ip and node's ip.
    Node location is known in advance. It's recorded in config file.
    :param user_ip: User IP
    :param active_nodes: Collection of active nodes: {node name: Node}
    :return: Node object
    """

    try:
        url_ip_location = (f"https://ipapi.co/{user_ip}/json/"
                           f"?key={settings.ipapi_key}")
        response = requests.get(url_ip_location)
        res = response.json()
        try:
            user_coordinates = (float(res['latitude']),
                                float(res['longitude']))
        except KeyError:
            logging.error(f"'{user_ip}' not found in the database.")
            return False
    except ClientConnectorError as err:
        logging.error(f"{err}")
        return False

    # Just taking first node from the collection
    closest_node = list(active_nodes.values())[0]
    # Calculate min distance for first element
    min_distance = distance(user_coordinates,
                            (closest_node.latitude,
                             closest_node.longitude))

    # Find min distance in the active nodes
    for node in list(active_nodes.values())[1:]:
        distance_ = distance(user_coordinates,
                             (node.latitude,
                              node.longitude))
        if distance_ < min_distance:
            min_distance = distance_
            closest_node = node

    logging.info(f"Use location {closest_node.endpoint}")
    return closest_node


def object_exists(client: Type[AbstractS3],
                  bucket_name: str,
                  object_name: str,
                  node: Node) -> bool:
    """
    Check if object exists in the bucket
    :param client: S3 client
    :param bucket_name: Bucket name
    :param object_name: Object name
    :param node: Node object
    :return:
    """
    client: AbstractS3 = client(endpoint=node.endpoint,
                                access_key=node.access_key_id,
                                secret_key=node.secret_access_key,
                                secure=False)

    bucket_found: bool = client.bucket_exists(bucket_name)
    if not bucket_found:
        return False

    object_found: Any = client.get_object(bucket_name,
                                          object_name,
                                          offset=0,
                                          length=1)
    if not object_found:
        return False

    return object_found


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
                        edge_node: Node) -> None:
    origin_client: AbstractS3 = client(
        endpoint=origin_node.endpoint,
        access_key=origin_node.access_key_id,
        secret_key=origin_node.secret_access_key,
        secure=False)

    obj = origin_client.stat_object(
        settings.bucket_name,
        object_name)

    edge_client_dict = {
        'endpoint_url': 'http://' + edge_node.endpoint,
        'aws_access_key_id': edge_node.access_key_id,
        'aws_secret_access_key': edge_node.secret_access_key,
        'verify': False}

    edge_client = S3MultipartUpload(settings.bucket_name,
                                    object_name,
                                    total_bytes=obj.size,
                                    content_type=obj.content_type,
                                    **edge_client_dict)

    multipart_upload(upload_client=edge_client,
                     origin_client=origin_client,
                     is_api=False)


def multipart_upload(upload_client: S3MultipartUpload,
                     object_: Any = None,
                     origin_client: AbstractS3 | None = None,
                     is_api: bool = True):
    upload_client.abort_all()
    # create new multipart upload
    mpu_id = upload_client.create()
    logging.info(f"Starting upload with id={mpu_id}")

    # upload parts
    parts = []
    part_number = 1
    uploaded_bytes = 0
    while True:
        if not is_api:
            got_obj = origin_client.get_object(
                settings.bucket_name,
                upload_client.key,
                uploaded_bytes,
                settings.upload_part_size)
            data = got_obj.data if got_obj else b''
        else:
            data = object_.read(settings.upload_part_size)
        if not len(data):
            break
        parts.extend(upload_client.upload_bytes(mpu_id,
                                                data,
                                                part_number))
        uploaded_bytes += len(data)
        logging.info(
            f"""{uploaded_bytes} of {upload_client.total_bytes} bytes \
uploaded {upload_client.as_percent(uploaded_bytes,
                                   upload_client.total_bytes)}%""")
        part_number += 1
    res = upload_client.complete(mpu_id, parts)
    logging.info(f"Upload completed with metadata: "
                 f"{res}")
    return res


if __name__ == "__main__":
    nodes = get_active_nodes("../.env.minio.json")
    print(find_closest_node('137.0.0.1', nodes))
