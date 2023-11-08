import os
from logging import config as logging_config

from pydantic import BaseSettings, Field

from core.logger import LOGGING

logging_config.dictConfig(LOGGING)


class MainConf(BaseSettings):
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class Settings(MainConf):
    project_name: str = Field(..., env='PROJECT_NAME')
    host: str = Field(..., env='HOST_CDN')
    port: int = Field(..., env='PORT_CDN')
    bucket_name: str = Field(..., env='BUCKET_NAME')
    upload_part_size: int = Field(..., env='UPLOAD_PART_SIZE')
    ipapi_key: str = Field(..., env='IPAPI_KEY')
    redis_host: str = Field(..., env='REDIS_HOST')
    redis_port: int = Field(..., env='REDIS_PORT')
    cache_expire_in_seconds: int = Field(..., env='CACHE_EXPIRE_IN_SECONDS')
    # host_auth: str = Field(..., env='HOST_AUTH')
    # port_auth: str = Field(..., env='PORT_AUTH')


settings = Settings()


class RateLimit(MainConf):
    request_limit_per_minute: int = Field(env="REQUEST_LIMIT_PER_MINUTE",
                                          default=20)
    is_rate_limit: bool = (os.getenv('IS_RATE_LIMIT', 'False') == 'True')


rl = RateLimit()


class CronSettings:
    finish_in_progress_tasks: dict = {
        'hour': 19,
        'minute': 2,
        'second': 3,
        'timezone': 'UTC'
    }
    abort_old_tasks: dict = {
        'hour': 18,
        'minute': 49,
        'second': 30,
        'timezone': 'UTC'
    }


cron_settings = CronSettings()
