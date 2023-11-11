import logging
from datetime import timedelta

import aiohttp
from aiohttp import ClientResponse
from fastapi import HTTPException, status
from miniopy_async.datatypes import Object
from miniopy_async import Minio, S3Error

from connectors.abstract import AbstractS3


class MinioS3(AbstractS3):
    def __init__(self, endpoint: str, *args, **kwargs):
        self.endpoint = endpoint
        self.client = Minio(endpoint=self.endpoint,
                            *args,
                            **kwargs)

    async def get_url(self, bucket_name, object_name, *args, **kwargs) -> str:
        try:
            url = await self.client.get_presigned_url(
                "GET",
                bucket_name=bucket_name,
                object_name=object_name,
                expires=timedelta(hours=1))
            logging.info(f'{url}')
            return url
        except S3Error as exc:
            print("S3 error occurred.", exc)
        except KeyError:
            logging.error(f"Minio.get_presigned_url() called with bad params: "
                          f"{kwargs}")

    async def bucket_exists(self, bucket_name: str) -> bool:
        try:
            return await self.client.bucket_exists(bucket_name)
        except S3Error as exc:
            logging.error(f"{exc}")

    async def get_object(self,
                         bucket_name: str,
                         object_name: str,
                         s3=None,
                         offset: int = 0,
                         length: int = 1,
                         *args,
                         **kwargs) -> bool | ClientResponse:
        try:
            async with aiohttp.ClientSession() as session:
                response = await self.client.get_object(
                    bucket_name,
                    object_name,
                    session,
                    offset,
                    length)
                logging.info(f"Found '{object_name}' in bucket "
                             f"'{bucket_name}' in S3 '{self.endpoint}'")
            return response
        except S3Error as e:
            logging.info(e)
            return False

    async def stat_object(self,
                          bucket_name: str,
                          object_name: str
                          ) -> Object:
        try:
            return await self.client.stat_object(bucket_name, object_name)
        except S3Error as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"{e}",
                headers={"WWW-Authenticate": "Bearer"},
            )

    async def copy_object(self, source: str, destination: str) -> None:
        pass

    async def fget_object(self, bucket_name: str, object_name: str,
                          file_name) -> bool:
        pass

    async def remove_object(self, bucket_name: str, object_name: str):
        try:
            response = await self.client.remove_object(
                bucket_name,
                object_name, )
            logging.info(f"Removed '{object_name}' from bucket "
                         f"'{bucket_name}' in S3 '{self.endpoint}'")
            return response
        except S3Error as e:
            logging.error(e)


minio_s3: MinioS3 | None = None


async def get_minio_s3() -> MinioS3:
    return minio_s3
