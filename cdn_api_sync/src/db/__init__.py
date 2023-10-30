from abc import ABC, abstractmethod
from typing import Optional
from models.model import Model


from aiohttp import ClientResponse
from minio.datatypes import Object
from urllib3.response import HTTPResponse


class AbstractS3(ABC):
    """
    Abstract class to work with S3 storage
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        self.client: Optional = None

    @abstractmethod
    def get_url(self, *args, **kwargs) -> str:
        """
        Get presigned url with to download or preview data
        :return:
        """
        pass

    @abstractmethod
    def bucket_exists(self, bucket_name) -> bool:
        pass

    @abstractmethod
    def get_object(self,
                   bucket_name: str,
                   object_name: str,
                   offset: int = 0,
                   length: int = 1,
                   *args,
                   **kwargs) -> bool | HTTPResponse:
        pass

    @abstractmethod
    def stat_object(self,
                    bucket_name: str,
                    object_name: str
                    ) -> Object:
        pass

    @abstractmethod
    def copy_object(self, source: str, destination: str) -> None:
        pass

    @abstractmethod
    def fget_object(self,
                    bucket_name: str,
                    object_name: str,
                    file_name) -> bool:
        pass

    @abstractmethod
    def remove_object(self,
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
    def insert_data(
            self,
            insert: Model | dict,
            collection: str,
    ) -> None:
        pass

    @abstractmethod
    def get_data(
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
    def update_data(
            self,
            query: dict,
            update: dict,
            collection: str,
    ) -> None:
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
    async def get_from_cache_by_id(self, _id: str, model) -> Optional:
        """
        Абстрактный асинхронный метод для получения данных по id из кэша
        :param _id: строка с id, по которой выполняется поиск
        :param model: тип модели, в котором возвращаются данные
        :return: объект типа, заявленного в model
        """
        ...

    @abstractmethod
    async def put_to_cache_by_id(self, entity):
        """
        Абстрактный асинхронный метод, который кладет данные в кэш по id
        :param entity: данные, которые кладем в кэш
        """
        ...

    @abstractmethod
    async def get_from_cache_by_key(self,
                                    model,
                                    key: str = None,
                                    sort: str = None) -> list | None:
        """
        Абстрактный асинхронный метод для получения данных по ключу из кэша
        :param model: тип модели, в котором возвращаются данные
        :param key: по данному ключу получаем данные из кэша
        :param sort: строка с названием атрибута, по которой необходима
        сортировка
        """
        ...

    @abstractmethod
    async def put_to_cache_by_key(self,
                                  key: str = None,
                                  entities: list = None):
        """
        Абстрактный асинхронный метод, который кладет данные в кэш по ключу
        :param key: по данному ключу записываются данные в кэш
        :param entities: данные, которые кладем в кэш
        """
        ...
