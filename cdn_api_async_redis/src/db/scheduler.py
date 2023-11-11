from typing import Any

from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from core.config import cron_settings
from db.abstract import AbstractCache
from db.aws_s3 import AWSS3, S3MultipartUpload
from services.scheduler import finish_in_progress_tasks, abort_old_tasks

scheduler: AsyncIOScheduler | None = AsyncIOScheduler()


async def get_scheduler() -> AsyncIOScheduler:
    return scheduler


async def jobs(scheduler_: AsyncIOScheduler,
               function: Any = None,
               *args,
               **kwargs) -> None | Job:
    """
    Add jobs to queue
    :param scheduler_: AsyncIOScheduler instance
    :param function: function to execute
    :return:
    """
    if not function:
        return None
    scheduler_.add_job(function, *args, **kwargs)


async def add_startup_jobs(scheduler_: AsyncIOScheduler,
                           cache: AbstractCache) -> None:
    """
    Add jobs to the application start up
    :param scheduler_: AsyncIOScheduler instance
    :param cache: Object cache
    :return:
    """
    await jobs(scheduler_,
               finish_in_progress_tasks,
               args=(AWSS3, cache),
               trigger='interval',
               minutes=cron_settings.finish_in_progress_tasks['minute'],
               )
    await jobs(scheduler_,
               abort_old_tasks,
               args=(S3MultipartUpload, cache),
               trigger='interval',
               minutes=cron_settings.abort_old_tasks['minute'],
               )