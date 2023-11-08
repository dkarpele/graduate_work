from functools import lru_cache

from fastapi import Depends

from db.aws_s3 import AWSS3, get_aws_s3
from db.minio_s3 import get_minio_s3, MinioS3
from services.service import CDNService


@lru_cache()
def get_minio_cdn_service(
        minio_s3: MinioS3 = Depends(get_minio_s3)) -> CDNService:
    return CDNService(minio_s3)


@lru_cache()
def get_aws_cdn_service(
        aws_s3: AWSS3 = Depends(get_aws_s3)) -> CDNService:
    return CDNService(aws_s3)
