import logging
import os
from datetime import datetime
from typing import Any

from aioboto3 import Session
from aiofiles import open
from fastapi import HTTPException, status
from minio import S3Error
from minio.datatypes import Object

from core.config import settings
from db import AbstractS3
from db import AbstractStorage


class AWSS3(AbstractS3):
    def __init__(self, endpoint: str, *args, **kwargs):
        self.session = Session()
        self.endpoint = endpoint
        self.client = self.session.client('s3',
                                          endpoint_url=self.endpoint,
                                          *args,
                                          **kwargs
                                          )

    async def get_url(self, *args, **kwargs) -> str:
        pass

    async def bucket_exists(self, bucket_name) -> bool:
        pass

    async def get_object(self,
                         bucket_name: str,
                         object_name: str,
                         s3=None,
                         offset: int = 0,
                         length: int = 1,
                         *args, **kwargs) -> dict | bool:
        try:
            range_ = f"bytes={offset}-{length}"
            response = await s3.get_object(
                Bucket=bucket_name,
                Key=object_name,
                Range=range_)
            logging.info(f"Found '{object_name}' in bucket "
                         f"'{bucket_name}' in S3 '{response['Body'].host}'")

            return response
        except S3Error as e:
            logging.info(e)
            return False

    async def stat_object(self,
                          bucket_name: str,
                          object_name: str
                          ) -> Object:
        try:
            return await self.client.select_object_content(bucket_name,
                                                           object_name)
        except S3Error as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"{e}",
                headers={"WWW-Authenticate": "Bearer"},
            )

    async def copy_object(self, source: str, destination: str) -> None:
        pass

    async def fget_object(self,
                          bucket_name: str,
                          object_name: str,
                          file_name) -> bool:
        try:
            async with self.client as s3:
                s3.download_file(bucket_name,
                                 object_name,
                                 file_name)

                logging.info(f"Object '{object_name}' downloaded from bucket "
                             f"'{bucket_name}' to '{file_name}'")

        except BaseException as e:
            logging.error(e)
            return False
        except FileNotFoundError as e:
            logging.error("Can't download file")
            logging.error(e)

    async def remove_object(self, bucket_name: str, object_name: str):
        pass


class S3MultipartUpload(AWSS3):
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM = int(5e6)

    def __init__(self,
                 bucket: str,
                 key: str,
                 local_path: str | None = None,
                 content_type: str | None = None,
                 total_bytes: int = 0,
                 part_size: int = settings.upload_part_size,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.path = local_path
        self.content_type = content_type
        if self.path:
            self.total_bytes = os.stat(local_path).st_size
        else:
            self.total_bytes = total_bytes
        self.part_bytes = part_size
        assert part_size > self.PART_MINIMUM

    async def abort_all(self, s3):
        mpus = await s3.list_multipart_uploads(Bucket=self.bucket)
        aborted = []
        if "Uploads" in mpus:
            logging.info(f"Aborting {len(mpus['Uploads'])} uploads")
            for u in mpus["Uploads"]:
                upload_id = u["UploadId"]  # also: Key
                aborted.append(
                    await s3.abort_multipart_upload(
                        Bucket=self.bucket, Key=self.key,
                        UploadId=upload_id))
        return aborted

    async def get_uploaded_parts(self, s3, upload_id: str) -> list:
        parts = []
        res = await s3.list_parts(Bucket=self.bucket, Key=self.key,
                                  UploadId=upload_id)
        if "Parts" in res:
            for p in res["Parts"]:
                parts.append(p)  # PartNumber, ETag, Size [bytes], ...
        return parts

    async def create(self, s3):
        mpu = await s3.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            ContentType=self.content_type)
        mpu_id = mpu["UploadId"]
        return mpu_id

    @staticmethod
    async def as_percent(num, denom):
        return round(float(num) / float(denom) * 100.0, 2)

    async def upload_file(self, s3, mpu_id, parts=None):
        if parts is None:
            parts = []
        uploaded_bytes = 0
        async with open(self.path, "rb") as f:
            i = 1
            while True:
                data = await f.read(self.part_bytes)
                if not len(data):
                    break

                if len(parts) >= i:
                    # Already uploaded, go to the next one
                    part = parts[i - 1]
                    if len(data) != part["Size"]:
                        raise Exception("Size mismatch: local " + str(
                            len(data)) + ", remote: " + str(part["Size"]))
                    parts[i - 1] = {k: part[k] for k in ('PartNumber', 'ETag')}
                else:

                    part = await s3.upload_part(
                        # We could include `ContentMD5='hash'` to discover
                        # if data has been corrupted upon transfer
                        Body=data,
                        Bucket=self.bucket,
                        Key=self.key,
                        UploadId=mpu_id,
                        PartNumber=i,
                    )

                    parts.append({"PartNumber": i, "ETag": part["ETag"]})

                uploaded_bytes += len(data)
                logging.info(
                    f"""{uploaded_bytes} of {self.total_bytes} bytes \
                uploaded {self.as_percent(uploaded_bytes,
                                          self.total_bytes)}%""")
                i += 1
        return parts

    async def upload_bytes(self,
                           s3,
                           mpu_id: str,
                           storage: AbstractStorage,
                           status_: str,
                           parts: list | None = None,
                           origin_client: AbstractS3 | None = None,
                           origin_client_s3=None,
                           object_: Any = None,
                           collection: str = "api",
                           ):

        object_name = self.key
        endpoint = self.endpoint
        query = {"object_name": object_name,
                 "node": endpoint}

        if parts is None:
            parts = []

        uploaded_bytes = 0
        part_number = 1
        while True:
            if collection == "cdn":
                offset = uploaded_bytes
                length = uploaded_bytes + settings.upload_part_size - 1
                if offset >= self.total_bytes:
                    data = b''
                else:
                    got_obj = await origin_client.get_object(
                        settings.bucket_name,
                        object_name,
                        s3=origin_client_s3,
                        offset=offset,
                        length=length)
                    data = await got_obj['Body'].content.readexactly(
                        got_obj['ContentLength'])

            elif collection == "api":
                data = await object_.read(settings.upload_part_size)
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Bad collection '{str(collection)}'!",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            if not len(data):
                break

            if len(parts) >= part_number:
                # Already uploaded, go to the next one
                part = parts[part_number - 1]
                if len(data) != part["Size"]:
                    raise Exception("Size mismatch: local " + str(
                        len(data)) + ", remote: " + str(part["Size"]))
                parts[part_number - 1] = {k: part[k]
                                          for k in ('PartNumber', 'ETag')}
            else:
                part = await s3.upload_part(
                    # We could include `ContentMD5='hash'` to discover if
                    # data has been corrupted upon transfer
                    Body=data,
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=mpu_id,
                    PartNumber=part_number,
                )

                # Uploading intermediate data to MongoDB
                update = {"object_name": object_name,
                          "node": endpoint,
                          "mpu_id": mpu_id,
                          "part_number": part_number,
                          "Etag": part["ETag"],
                          "uploaded": uploaded_bytes,
                          "size": self.total_bytes,
                          "last_modified": datetime.utcnow(),
                          "status": status_}
                await storage.update_data(query=query,
                                          update=update,
                                          collection=collection)

                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
            uploaded_bytes += len(data)
            logging.info(
                f"""{uploaded_bytes} of {self.total_bytes} bytes \
            uploaded {await self.as_percent(uploaded_bytes,
                                            self.total_bytes)}%
            """)
            part_number += 1

        return parts

    async def complete(self, s3, mpu_id, parts):
        result = await s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts})
        logging.info(f"complete: parts={str(parts)}")
        return result


aws_s3: AWSS3 | None = None


async def get_aws_s3() -> AWSS3:
    return aws_s3
