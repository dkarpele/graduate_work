import logging
from datetime import timedelta

from minio import Minio, S3Error
from minio.datatypes import Object
from urllib3.response import HTTPResponse

from db import AbstractS3


class MinioS3(AbstractS3):
    def __init__(self, **kwargs):
        self.client = Minio(**kwargs)

    @property
    def base_url(self):
        return self.client._base_url

    def get_url(self, bucket_name, object_name, *args, **kwargs) -> str:
        try:
            url = self.client.get_presigned_url(
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

    def bucket_exists(self, bucket_name: str) -> bool:
        try:
            return self.client.bucket_exists(bucket_name)
        except S3Error as exc:
            print("S3 error occurred.", exc)

    def get_object(self,
                   bucket_name: str,
                   object_name: str,
                   offset: int = 0,
                   length: int = 1,
                   *args,
                   **kwargs) -> bool | HTTPResponse:
        try:
            response = self.client.get_object(
                bucket_name,
                object_name,
                offset,
                length)
            logging.info(f"Found '{object_name}' in bucket "
                         f"'{bucket_name}' in S3 '{self.base_url.host}'")
            return response
        except S3Error as e:
            logging.warning(f"{object_name} doesn't exist in bucket "
                            f"{bucket_name}")
            logging.warning(e)
            return False

    def stat_object(self,
                    bucket_name: str,
                    object_name: str
                    ) -> Object:
        return self.client.stat_object(bucket_name, object_name)

    def copy_object(self, source: str, destination: str) -> None:
        pass

    def fget_object(self, bucket_name: str, object_name: str,
                    file_name) -> bool:
        pass


minio_s3: MinioS3 | None = None


def get_minio_s3() -> MinioS3:
    return minio_s3
