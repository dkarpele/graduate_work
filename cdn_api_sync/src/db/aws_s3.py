import logging
import os
from typing import Any

import boto3
from fastapi import HTTPException, status
from minio import S3Error
from minio.datatypes import Object
from urllib3 import HTTPResponse

from core.config import settings
from db import AbstractS3
from db import AbstractStorage


class AWSS3(AbstractS3):
    def __init__(self, *args, **kwargs):
        self.client = boto3.client('s3',
                                   *args,
                                   **kwargs
                                   )

    def get_url(self, *args, **kwargs) -> str:
        pass

    def bucket_exists(self, bucket_name) -> bool:
        pass

    def get_object(self,
                   bucket_name: str,
                   object_name: str,
                   offset: int = 0,
                   length: int = 1,
                   *args, **kwargs) -> bool | HTTPResponse:
        try:
            response = self.client.get_object(Bucket=bucket_name,
                                              Key=object_name)
            logging.info(f"Found '{object_name}' in bucket "
                         f"'{bucket_name}' in S3 '{response}'")
            return response
        except S3Error as e:
            logging.info(e)
            return False

    def stat_object(self,
                    bucket_name: str,
                    object_name: str
                    ) -> Object:
        pass

    def copy_object(self, source: str, destination: str) -> None:
        pass

    def fget_object(self,
                    bucket_name: str,
                    object_name: str,
                    file_name) -> bool:
        try:
            self.client.download_file(bucket_name,
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

    def remove_object(self, bucket_name: str, object_name: str):
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
        # assert (self.total_bytes % part_size == 0
        #         or self.total_bytes % part_size > self.PART_MINIMUM)

    def abort_all(self):
        mpus = self.client.list_multipart_uploads(Bucket=self.bucket)
        aborted = []
        if "Uploads" in mpus:
            logging.info(f"Aborting {len(mpus['Uploads'])} uploads")
            for u in mpus["Uploads"]:
                upload_id = u["UploadId"]  # also: Key
                aborted.append(
                    self.client.abort_multipart_upload(
                        Bucket=self.bucket, Key=self.key,
                        UploadId=upload_id))
        return aborted

    def get_uploaded_parts(self, upload_id: str) -> list:
        parts = []
        res = self.client.list_parts(Bucket=self.bucket, Key=self.key,
                                     UploadId=upload_id)
        if "Parts" in res:
            for p in res["Parts"]:
                parts.append(p)  # PartNumber, ETag, Size [bytes], ...
        return parts

    def create(self):
        mpu = self.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            ContentType=self.content_type)
        mpu_id = mpu["UploadId"]
        return mpu_id

    @staticmethod
    def as_percent(num, denom):
        return round(float(num) / float(denom) * 100.0, 2)

    def upload_file(self, mpu_id, parts=None):
        if parts is None:
            parts = []
        uploaded_bytes = 0
        with open(self.path, "rb") as f:
            i = 1
            while True:
                data = f.read(self.part_bytes)
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

                    part = self.client.upload_part(
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

    def upload_bytes(self,
                     mpu_id: str,
                     storage: AbstractStorage,
                     parts: list | None = None,
                     origin_client: AbstractS3 | None = None,
                     object_: Any = None,
                     collection: str = "api"):
        object_name = self.key
        query = {"object_name": object_name,
                 "node": self.client.meta.endpoint_url}

        if parts is None:
            parts = []

        uploaded_bytes = 0
        part_number = 1
        while True:
            if collection == "cdn":
                got_obj = origin_client.get_object(
                    settings.bucket_name,
                    object_name,
                    uploaded_bytes,
                    settings.upload_part_size)
                data = got_obj.data if got_obj else b''
            elif collection == "api":
                data = object_.read(settings.upload_part_size)
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
                part = self.client.upload_part(
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
                          "node": self.client.meta.endpoint_url,
                          "mpu_id": mpu_id,
                          "part_number": part_number,
                          "Etag": part["ETag"],
                          "status": "in_progress"}
                storage.update_data(query=query,
                                    update=update,
                                    collection=collection)

                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
            uploaded_bytes += len(data)
            logging.info(
                f"""{uploaded_bytes} of {self.total_bytes} bytes \
            uploaded {self.as_percent(uploaded_bytes,
                                      self.total_bytes)}%
            """)
            part_number += 1

        return parts

    def complete(self, mpu_id, parts):
        logging.info(f"complete: parts={str(parts)}")
        result = self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts})
        return result


aws_s3: AWSS3 | None = None


def get_aws_s3() -> AWSS3:
    return aws_s3
