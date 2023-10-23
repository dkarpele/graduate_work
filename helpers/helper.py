import asyncio
import logging
from dataclasses import dataclass
from json import JSONDecodeError, loads
from typing import Any, Type

import aiofiles
import aiohttp
from aiohttp import ClientConnectorError
from dotenv import load_dotenv
from geopy.distance import distance

from db import AbstractS3
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


async def get_active_nodes(
        file_path: str = "./.env.minio.json"
) -> dict[str, Node]:
    """
    Get origin and all active edge locations (is_active = True)
    :param file_path: Edge and origin configuration
    :return: Collection of {node name: Node}
    """
    try:
        async with aiofiles.open(file_path, "r") as file:
            j = loads(await file.read())
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


async def origin_is_alive(nodes: dict[str, Node]):
    try:
        return nodes['ORIGIN']
    except KeyError:
        logging.error("ORIGIN node died. Failing")
        raise locations_not_available


async def find_closest_node(user_ip: str,
                            active_nodes: dict[str, Node]) -> Node | bool:
    """
    Get node that has the shortest distance between user's ip and node's ip.
    Node location is known in advance. It's recorded in config file.
    :param user_ip: User IP
    :param active_nodes: Collection of active nodes: {node name: Node}
    :return: Node object
    """

    async with aiohttp.ClientSession() as session:
        try:
            url_ip_location = f"https://ipapi.co/{user_ip}/json/"
            async with session.get(url_ip_location) as response:
                res = await response.json()
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


async def object_exists(client: Type[AbstractS3],
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

    bucket_found: bool = await client.bucket_exists(bucket_name)
    if not bucket_found:
        return False

    object_found: Any = await client.get_object(bucket_name, object_name)
    if not object_found:
        return False

    return object_found


async def main():
    nodes = await get_active_nodes("../.env.minio.json")
    print(await find_closest_node('137.0.0.1', nodes))

if __name__ == "__main__":
    asyncio.run(main())
