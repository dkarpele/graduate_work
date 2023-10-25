import logging

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from api.v1 import films
from core.config import settings
from core.logger import LOGGING
from db import elastic, redis, minio_s3
from db.scheduler import jobs, get_scheduler


async def startup():
    # Connecting to scheduler
    job = await get_scheduler()
    await jobs(job)
    job.start()
    logging.info(f'List of scheduled jobs: {job.get_jobs()}')

    # minio_s3.minio_s3 = minio_s3.MinioS3(endpoint="127.0.0.1:9000",
    #                                      access_key="minioadmin",
    #                                      secret_key="minioadmin",
    #                                      secure=False)
    # redis.redis = redis.Redis(host=settings.redis_host,
    #                           port=settings.redis_port,
    #                           ssl=False)


async def shutdown():
    pass
    # await redis.redis.close()
    # await elastic.es.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await startup()
    yield
    await shutdown()


app = FastAPI(
    title=settings.project_name,
    description="CDN API. Could be used to upload/download data to S3",
    version="1.0.0",
    docs_url='/api/openapi-cdn',
    openapi_url='/api/openapi-cdn.json',
    default_response_class=ORJSONResponse,
    lifespan=lifespan)

app.include_router(films.router, prefix='/api/v1/films', tags=['films'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=f'{settings.host}',
        port=settings.port,
        log_config=LOGGING,
        log_level=logging.DEBUG
    )
