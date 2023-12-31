from abc import ABC, abstractmethod
from typing import Optional

from miniopy_async.datatypes import Object
from pymongo.results import DeleteResult

from models.model import Model


class AbstractS3(ABC):
    """
    Abstract class to work with S3 storage
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        self.endpoint: str | None = None
        self.client: Optional = None

    @abstractmethod
    async def get_url(self, *args, **kwargs) -> str:
        """
        Get presigned url with to download or preview data
        :return:
        """
        pass

    @abstractmethod
    async def bucket_exists(self, bucket_name) -> bool:
        pass

    @abstractmethod
    async def get_object(self,
                         bucket_name: str,
                         object_name: str,
                         s3=None,
                         offset: int = 0,
                         length: int = 1,
                         size: int = 1,
                         *args,
                         **kwargs) -> dict | bool:
        pass

    @abstractmethod
    async def stat_object(self,
                          bucket_name: str,
                          object_name: str
                          ) -> Object:
        pass

    @abstractmethod
    async def copy_object(self, source: str, destination: str) -> None:
        pass

    @abstractmethod
    async def fget_object(self,
                          bucket_name: str,
                          object_name: str,
                          file_name) -> bool:
        pass

    @abstractmethod
    async def remove_object(self,
                            bucket_name: str,
                            object_name: str,
                            ):
        pass


class AbstractStorage(ABC):
    """
    Абстрактный класс для работы с хранилищем данных.
    Описывает какие методы должны быть у подобных классов.
    get_by_id - возвращает один экземпляр класса модели,
    по которой строятся получение из хранилища
    get_list - возвращает список объектов модели, переданной в
    качестве параметра.
    """

    @abstractmethod
    async def insert_data(
            self,
            insert: Model | dict,
            collection: str,
    ) -> None:
        pass

    @abstractmethod
    async def get_data(
            self,
            query: dict | str,
            collection: str,
            projection: dict | None = None,
            sort: tuple | None = None,
            page: int | None = None,
            size: int | None = None
    ) -> list:
        pass

    @abstractmethod
    async def update_data(
            self,
            query: dict,
            update: dict,
            collection: str,
    ) -> None:
        pass

    @abstractmethod
    async def delete_data(
            self,
            document: Model | dict,
            collection: str,
    ) -> DeleteResult:
        pass


class AbstractCache(ABC):
    """
    Абстрактный класс для работы с кэшем.
    Описывает какие методы должны быть у подобных классов.
    :get_from_cache_by_id - возвращает один экземпляр класса модели,
    по которой строятся получение из кэша по id
    :get_from_cache_by_key - возвращает один экземпляр класса модели,
    по которой строятся получение из кэша по ключу
    :put_to_cache_by_id - кладет данные в кэш по id.
    :put_to_cache_by_key - кладет данные в кэш по ключу.
    """

    @abstractmethod
    async def close(self):
        """
        Абстрактный асинхронный метод для закрытия соединения
        """
        ...

    @abstractmethod
    async def get_from_cache_by_id(self, _id: str) -> Optional:
        """
        Абстрактный асинхронный метод для получения данных по id из кэша
        :param _id: строка с id, по которой выполняется поиск
        :return: объект типа, заявленного в model
        """
        ...

    @abstractmethod
    async def put_to_cache_by_id(self, _id, entity, expire):
        """
        Абстрактный асинхронный метод, который кладет данные в кэш по id
        :param _id:
        :param entity: данные, которые кладем в кэш
        :param expire: время жизни записи
        """
        ...

    @abstractmethod
    async def delete_from_cache_by_id(self, _id):
        """
        Абстрактный асинхронный метод, который удаляет данные из кэша по id
        :param _id:
        """
        ...

    @abstractmethod
    async def get_from_cache_by_key(self,
                                    key: str = None,
                                    sort: str = None) -> dict | None:
        """
        Абстрактный асинхронный метод для получения данных по ключу из кэша
        :param key: по данному ключу получаем данные из кэша
        :param sort: строка с названием атрибута, по которой необходима
        сортировка
        """
        ...

    @abstractmethod
    async def put_to_cache_by_key(self,
                                  key: str = None,
                                  entities: dict = None):
        """
        Абстрактный асинхронный метод, который кладет данные в кэш по ключу
        :param key: по данному ключу записываются данные в кэш
        :param entities: данные, которые кладем в кэш
        """
        ...

    @abstractmethod
    async def get_keys_by_pattern(self,
                                  pattern: str = None, ):
        """
        Get cache keys using pattern
        :param pattern: str
        :return: AsyncIterator with keys
        """
        ...

    @abstractmethod
    async def get_pipeline(self):
        """
        Creates pipeline
        :return:
        """
        ...
