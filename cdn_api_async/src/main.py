import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from api.v1 import films
from core.config import settings, mongo_settings, cron_settings
from core.logger import LOGGING
from db import mongo
from db.aws_s3 import AWSS3
from db.scheduler import jobs, get_scheduler
from services.scheduler import finish_in_progress_tasks


async def startup():
    if mongo_settings.user and mongo_settings.password:
        mongo.mongo = mongo.Mongo(
            f'mongodb://'
            f'{mongo_settings.user}:{mongo_settings.password}@'
            f'{mongo_settings.host}:{mongo_settings.port}'
        )
    else:
        mongo.mongo = mongo.Mongo(
            f'mongodb://'
            f'{mongo_settings.host}:{mongo_settings.port}'
        )

    # Connecting to scheduler
    job = await get_scheduler()
    await jobs(job,
               finish_in_progress_tasks,
               args=(AWSS3, mongo.mongo),
               trigger='cron',
               minute=cron_settings.finish_in_progress_tasks['minute'],
               second=cron_settings.finish_in_progress_tasks['second'],
               timezone=cron_settings.finish_in_progress_tasks['timezone']
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
