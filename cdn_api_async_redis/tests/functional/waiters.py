import asyncio

from logging import config as logging_config

from utils.logger import LOGGING
from utils.wait_for_es import wait_es
from utils.wait_for_redis import wait_redis
# Применяем настройки логирования
logging_config.dictConfig(LOGGING)


async def main():
    await asyncio.gather(wait_es(), wait_redis())

if __name__ == '__main__':
    asyncio.run(main())
