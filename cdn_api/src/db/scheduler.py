from typing import Any

from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler


scheduler: AsyncIOScheduler | None = AsyncIOScheduler()


async def get_scheduler() -> AsyncIOScheduler:
    return scheduler


async def jobs(job: AsyncIOScheduler, function: Any = None, *args, **kwargs)\
        -> None | Job:
    """
    List of jobs to schedule
    :param function: function to execute
    :param job: job name as a function
    :return:
    """
    if not function:
        return None
    job.add_job(function, *args, **kwargs)
