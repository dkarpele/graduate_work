from fastapi.encoders import jsonable_encoder
from pymongo import MongoClient, errors
from pymongo.results import DeleteResult

from core.config import mongo_settings
from helpers.exceptions import entity_doesnt_exist
from models.model import Model


class Mongo:
    MAX_PAGE_SIZE = 10

    def __init__(self, client: str):
        self.client = MongoClient(client)

    def insert_data(
            self,
            insert: Model | dict,
            collection: str,
    ) -> None:
        """Save doc in Mongo db
        Args:
            :param insert: Document to insert
            :param collection: Collection name
        """
        db_name = mongo_settings.db
        db = self.client[db_name]
        try:
            db[collection].insert_one(jsonable_encoder(insert))
        except errors.PyMongoError as err:
            raise entity_doesnt_exist(err)

    def update_data(
            self,
            query: dict,
            update: dict,
            collection: str,
    ) -> None:
        """Save doc in Mongo db
        Args:
            :param update: Document to upload
            :param query: document to match
            :param collection: Collection name
        """
        db_name = mongo_settings.db
        db = self.client[db_name]
        try:
            db[collection].update_one(query,
                                      {"$set": update},
                                      upsert=True)
        except errors.PyMongoError as err:
            raise entity_doesnt_exist(err)

    def get_data(
            self,
            query: dict | str,
            collection: str,
            projection: dict | None = None,
            sort: tuple | None = None,
            page: int | None = None,
            size: int | None = None
    ) -> list:
        """Get doc from Mongo db
        Args:
            :param page: page number
            :param size: page size
            :param sort: tuple = ('sort_by', 1 or -1 for asc or desc)
            :param projection: which fields are returned in the matching
            documents.
            :param query: Request to find
            :param collection: Collection name
        """
        if not sort:
            sort = ('_id', 1)

        if page and size:
            offset = (page * size) - size
        elif page and not size:
            size = self.MAX_PAGE_SIZE
            offset = (page * size) - size
        elif not page and size:
            offset = 0
        else:
            offset = 0
            size = self.MAX_PAGE_SIZE

        db_name = mongo_settings.db
        db = self.client[db_name]

        res = (db[collection].
               find(query, projection).
               sort(*sort).
               skip(offset).
               limit(size))

        documents_list = []
        for document in res:
            try:
                document['_id'] = str(document['_id'])
            except KeyError:
                pass
            documents_list.append(document)

        return documents_list

    def delete_data(
            self,
            document: Model | dict,
            collection: str,
    ) -> DeleteResult:
        """Delete doc from collection in Mongo db
        Args:
            :param document: Document to delete
            :param collection: Collection name
        """
        db_name = mongo_settings.db
        db = self.client[db_name]
        try:
            res = db[collection].delete_one(jsonable_encoder(document), )
        except errors.PyMongoError as err:
            raise entity_doesnt_exist(err)
        return res

    def get_aggregated(
            self,
            query: dict | str | list,
            collection: str,
    ) -> list:
        """Get aggregated doc from Mongo db
        Args:
            :param query: Request to find
            :param collection: Collection name
        """
        db_name = mongo_settings.db
        db = self.client[db_name]
        res = db[collection].aggregate(query, )
        documents_list = []
        for document in res:
            try:
                document['_id'] = str(document['_id'])
            except KeyError:
                pass
            documents_list.append(document)

        return documents_list

    def get_count(
            self,
            query: dict | str | list,
            collection: str,
    ) -> int:
        """Get doc's count from Mongo db
        Args:
            :param query: Request to find
            :param collection: Collection name
        """
        db_name = mongo_settings.db
        db = self.client[db_name]
        count = db[collection].count_documents(query)

        return count


mongo: Mongo | None = None


def get_mongo() -> Mongo | None:
    return mongo
