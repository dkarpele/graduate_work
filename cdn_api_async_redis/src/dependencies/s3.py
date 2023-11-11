from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from connectors.abstract import AbstractS3
from connectors.aws_s3 import AWSS3, get_aws_s3
from connectors.minio_s3 import get_minio_s3, MinioS3


@lru_cache()
def get_minio_cdn_service(
        minio_s3: MinioS3 = Depends(get_minio_s3)) -> AbstractS3:
    return minio_s3


MinioDep = Annotated[AbstractS3, Depends(get_minio_cdn_service)]


@lru_cache()
def get_aws_cdn_service(
        aws_s3: AWSS3 = Depends(get_aws_s3)) -> AbstractS3:
    return aws_s3


AWSDep = Annotated[AbstractS3, Depends(get_aws_cdn_service)]
