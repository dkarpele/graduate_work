import asyncio
import logging
from json import JSONDecodeError, loads
from typing import Any, Type

from aiofiles import open
from aiohttp import ClientConnectorError, ClientSession
from dotenv import load_dotenv
from geopy.distance import distance

from core.config import settings
from db import AbstractS3, AbstractStorage, AbstractCache
from helpers.exceptions import locations_not_available
from models.model import Node, Model

load_dotenv()


async def get_active_nodes(
        file_path: str = "./.env.minio.json"
) -> dict[str, Node]:
    """
    Get origin and all active edge locations (is_active = True)
    :param file_path: Edge and origin configuration
    :return: Collection of {node name: Node}
    """
    try:
        async with open(file_path, "r") as file:
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
    async with ClientSession() as session:
        try:
            url_ip_location = (f"https://ipapi.co/{user_ip}/json/"
                               f"?key={settings.ipapi_key}")
            async with session.get(url_ip_location) as response:
                res = await response.json()
                try:
                    user_coordinates = (float(res['latitude']),
                                        float(res['longitude'])
                                        )
                except KeyError:
                    logging.error(f"'{user_ip}' not found in the database.")
                    return False
        except ClientConnectorError as err:
            logging.error(err)
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

    object_found: Any = await client.get_object(bucket_name,
                                                object_name,
                                                offset=0,
                                                length=1)
    if not object_found:
        return False

    return object_found


async def get_mpu_id(endpoint: str,
                     filename: str,
                     cache: AbstractCache,
                     collection: str = "api"):
    # If upload was failed - try to re-upload it with current mpu_id
    key: str = f"{collection}^{filename}^{endpoint}"

    res = await cache.get_from_cache_by_key(key)
    try:
        if str(res[b'status'], 'utf-8') == 'in_progress':
            return str(res[b'mpu_id'], 'utf-8')
    except TypeError:
        return None


async def is_scheduler_in_progress(cache: AbstractCache,
                                   node: Node,
                                   object_: str,
                                   collection: str) -> dict | None:
    endpoint = 'http://' + node.endpoint
    key: str = f"{collection}^{object_}^{endpoint}"

    res = await cache.get_from_cache_by_key(key)
    try:
        if str(res[b'status'], 'utf-8') == 'scheduler_in_progress':
            return res
        else:
            return None
    except TypeError:
        return None


async def get_in_progress_objects(cache: AbstractCache,
                                  collection: str,
                                  threshold: dict) -> list:

    query = {'$or': [{'status': 'in_progress'},
                     {'status': 'scheduler_in_progress'}
                     ],
             'last_modified': threshold
             }
    projection = {'object_name': 1,
                  'node': 1, }
    res = await cache.get_data(query=query,
                                 collection=collection,
                                 projection=projection,
                                 )
    return res


async def main():
    print(await find_closest_node('137.0.0.1',
                                  await get_active_nodes(
                                      "../../../.env.minio.json")))


if __name__ == "__main__":
    asyncio.run(main())
