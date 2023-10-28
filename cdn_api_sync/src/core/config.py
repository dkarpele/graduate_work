from logging import config as logging_config

from pydantic import BaseSettings, Field

from core.logger import LOGGING

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


class Settings(BaseSettings):
    project_name: str = Field(..., env='PROJECT_NAME')
    # redis_host: str = Field(..., env='REDIS_HOST')
    # redis_port: int = Field(..., env='REDIS_PORT')
    # elastic_host: str = Field(..., env='ELASTIC_HOST')
    # elastic_port: int = Field(..., env='ELASTIC_PORT')
    host: str = Field(..., env='HOST_CDN')
    port: int = Field(..., env='PORT_CDN')
    bucket_name: str = Field(..., env='BUCKET_NAME')
    upload_part_size: int = Field(..., env='UPLOAD_PART_SIZE')
    ipapi_key: str = Field(..., env='IPAPI_KEY')
    # host_auth: str = Field(..., env='HOST_AUTH')
    # port_auth: str = Field(..., env='PORT_AUTH')
    # cache_expire_in_seconds: int = Field(..., env='CACHE_EXPIRE_IN_SECONDS')

    class Config:
        env_file = '.env'


settings = Settings()


SORT_DESC = "Сортировка. По умолчанию по возрастанию." \
            "'-' в начале - по убыванию."
SEARCH_DESC = "Поиск по названию"
PAGE_DESC = "Номер страницы"
PAGE_ALIAS = "page_number"
SIZE_DESC = "Количество элементов на странице"
SIZE_ALIAS = "page_size"
