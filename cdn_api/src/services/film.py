from functools import lru_cache
from fastapi import Depends

from db.elastic import get_elastic, Elastic
from db.redis import get_redis, Redis
from db.minio_s3 import get_minio_s3, MinioS3
from models.films import Film
from services.service import IdRequestService, ListService, CDNService


@lru_cache()
def get_minio_cdn_service(
        minio_s3: MinioS3 = Depends(get_minio_s3)) -> CDNService:
    return CDNService(minio_s3)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: Elastic = Depends(get_elastic)) -> IdRequestService:
    return IdRequestService(redis, elastic, Film)


@lru_cache()
def get_film_list_service(
        redis: Redis = Depends(get_redis),
        elastic: Elastic = Depends(get_elastic)) -> ListService:
    return ListService(redis, elastic, Film)
