import asyncio
import logging
from datetime import timedelta

import aiohttp
from miniopy_async import Minio, S3Error
from urllib3.response import HTTPResponse

from db import AbstractS3


class MinioS3(AbstractS3):
    def __init__(self, **kwargs):
        self.client = Minio(**kwargs)

    async def get_url(self, bucket_name, object_name, *args, **kwargs) -> str:
        try:
            url = await self.client.get_presigned_url(
                "GET",
                bucket_name=bucket_name,
                object_name=object_name,
                expires=timedelta(hours=1))
            return url
        except S3Error as exc:
            print("S3 error occurred.", exc)
        except KeyError:
            logging.error(f"Minio.get_presigned_url() called with bad params: "
                          f"{kwargs}")

    async def bucket_exists(self, bucket_name: str) -> bool:
        try:
            async with aiohttp.ClientSession():
                return await self.client.bucket_exists(bucket_name)
        except S3Error as exc:
            print("S3 error occurred.", exc)

    async def get_object(self,
                         bucket_name: str,
                         object_name: str,
                         offset: int = 0,
                         length: int = 0,
                         *args,
                         **kwargs) -> bool | HTTPResponse:
        async with aiohttp.ClientSession() as session:
            try:
                response = await self.client.get_object(
                    bucket_name,
                    object_name,
                    session,
                    offset=offset,
                    length=length)
                # r = await response.read()
                logging.info(f"Found '{object_name}' in bucket "
                             f"'{bucket_name}' in S3 '{response.host}'")
                return response
            except S3Error as e:
                logging.warning(f"{object_name} doesn't exist in bucket "
                                f"{bucket_name}")
                logging.warning(e)
                return False

    async def copy_object(self, source: str, destination: str) -> None:
        pass

    async def fget_object(self, s3, bucket_name: str, object_name: str,
                          file_name) -> bool:
        pass


minio_s3: MinioS3 | None = None


async def get_minio_s3() -> MinioS3:
    return minio_s3
