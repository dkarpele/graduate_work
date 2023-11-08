import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Depends
from fastapi.responses import ORJSONResponse

from api.v1 import films
from core.config import settings, cron_settings
from core.logger import LOGGING
from db import redis
from db.aws_s3 import AWSS3, S3MultipartUpload
from db.scheduler import jobs, get_scheduler
from services.redis import rate_limit
from services.scheduler import finish_in_progress_tasks, abort_old_tasks


async def startup():
    redis.redis = redis.Redis(host=settings.redis_host,
                              port=settings.redis_port,
                              ssl=False)

    # Connecting to scheduler
    job = await get_scheduler()
    await jobs(job,
               finish_in_progress_tasks,
               args=(AWSS3, redis.redis),
               # trigger='cron',
               trigger='interval',
               minute=cron_settings.finish_in_progress_tasks['minute'],
               second=cron_settings.finish_in_progress_tasks['second'],
               timezone=cron_settings.finish_in_progress_tasks['timezone']
               )
    await jobs(job,
               abort_old_tasks,
               args=(S3MultipartUpload, redis.redis),
               # trigger='cron',
               trigger='interval',
               minute=cron_settings.abort_old_tasks['minute'],
               second=cron_settings.abort_old_tasks['second'],
               timezone=cron_settings.abort_old_tasks['timezone']
               )
    job.start()
    logging.info(f'List of scheduled jobs: {job.get_jobs()}')


async def shutdown():
    job = await get_scheduler()
    job.shutdown()


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
    lifespan=lifespan,
    dependencies=[Depends(rate_limit)])

app.include_router(films.router, prefix='/api/v1/films', tags=['films'])

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=f'{settings.host}',
        port=settings.port,
        log_config=LOGGING,
        log_level=logging.DEBUG
    )
