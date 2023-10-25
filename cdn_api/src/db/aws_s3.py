import asyncio
import logging
from datetime import timedelta

import aiohttp
from aioboto3 import Session
from aiofiles import open
import os
from aiohttp import ClientResponse
from miniopy_async import Minio, S3Error
from miniopy_async.helpers import ObjectWriteResult
from urllib3.response import HTTPResponse
from botocore.errorfactory import BaseClientExceptions

from db import AbstractS3


class AWSS3(AbstractS3):
    def __init__(self, *args, **kwargs):
        self.session = Session()
        self.client = self.session.client('s3',
                                          *args,
                                          **kwargs
                                          )

    async def get_url(self, *args, **kwargs) -> str:
        pass

    async def bucket_exists(self, bucket_name) -> bool:
        pass

    async def get_object(self, bucket_name: str,
                         object_name: str) -> bool | ClientResponse:
        pass

    async def copy_object(self, source: str, destination: str) -> None:
        pass

    async def fget_object(self,
                          bucket_name: str,
                          object_name: str,
                          file_name) -> bool:
        try:
            async with self.client as s3:
                await s3.download_file(bucket_name,
                                       object_name,
                                       file_name)

                logging.info(f"Object '{object_name}' downloaded from bucket "
                             f"'{bucket_name}' as '{file_name}'")

        except BaseException as e:
            logging.error(e)
            return False
        except FileNotFoundError as e:
            logging.error("Can't download file")
            logging.error(e)


class S3MultipartUpload(AWSS3):
    # AWS throws EntityTooSmall error for parts smaller than 5 MB
    PART_MINIMUM = int(5e6)

    def __init__(self, bucket, key, local_path, content_type,
                 part_size=int(6e6), profile_name=None,
                 region_name="eu-west-1", verbose=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.path = local_path
        self.content_type = content_type
        self.total_bytes = os.stat(local_path).st_size
        self.part_bytes = part_size
        assert part_size > self.PART_MINIMUM
        # assert (self.total_bytes % part_size == 0
        #         or self.total_bytes % part_size > self.PART_MINIMUM)
        # self.s3 = boto3.client('s3',
        #                        endpoint_url='http://192.168.16.5:9000',
        #                        aws_access_key_id='minioadmin',
        #                        aws_secret_access_key='minioadmin',
        #                        aws_session_token=None,
        #                        config=boto3.session.Config(
        #                            signature_version='s3v4'),
        #                        verify=False
        #                        )
        # self.s3 = boto3.session.Session(
        #     profile_name=profile_name, region_name=region_name).client("s3")
        # if verbose:
        #     boto3.set_stream_logger(name="botocore")

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

    async def get_uploaded_parts(self, upload_id):
        parts = []
        async with self.client as s3:
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
    def as_percent(num, denom):
        return float(num) / float(denom) * 100.0

    async def upload(self, s3, mpu_id, parts=None):
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
                logging.info(f"{uploaded_bytes} of {self.total_bytes} bytes "
                             f"uploaded "
                             f"{self.as_percent(uploaded_bytes, self.total_bytes)}%")
                i += 1
        return parts

    async def complete(self, s3, mpu_id, parts):
        print("complete: parts=" + str(parts))
        result = await s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=mpu_id,
                MultipartUpload={"Parts": parts})
        return result


aws_s3: AWSS3 | None = None


async def get_aws_s3() -> AWSS3:
    return aws_s3
