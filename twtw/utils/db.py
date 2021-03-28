import psycopg2
from psycopg2.extras import Json
from pymongo import MongoClient


class DBHelperPostgres:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = None,
        database: str = "mydb",
        table: str = "tweets",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.table = table
        self.conn: psycopg2.extensions.connection = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )

        self.cur: psycopg2.extensions.cursor = self.conn.cursor()

    def insert_tweet(self, tweet: dict) -> None:
        table_name = self.table

        item = dict(
            tweet_id=tweet["id"],
            tweet_json=Json(tweet),
            tweet_text=tweet["text"],
            tweet_user_id=tweet["user"]["id"],
        )

        sql = ("INSERT INTO %s (%s) " "VALUES (%%(%s)s );") % (table_name, ", ".join(item), ")s, %(".join(item))

        self.cur.execute(sql)
        self.conn.commit()

    def is_connected(self):
        raise NotImplementedError


class DBHelperMongo:
    def __init__(self, mongodb_url: str = "mongodb://127.0.0.1:27017", database: str = "mydb", table: str = "tweets"):

        self.client = MongoClient(mongodb_url)
        self.db = self.client[database]
        self.collection = self.db[table]

    @property
    def conn(self):
        return self.client

    def insert_tweet(self, tweet: dict):
        tweet["_id"] = tweet["id"]  # a default MongoDB index
        return self.collection.insert_one(tweet)

    def update_tweet(self, tweet: dict):
        if "_id" not in tweet:
            tweet["_id"] = tweet["id"]
        raise NotImplementedError("TODO:")  # TODO:

    def find(self, tweet_id: int):
        return self.collection.find({"_id": tweet_id})

    def count_documents_with_geo(self):
        return self.collection.count_documents({"geo": {"$exists": True, "$ne": None}})

    def get_documents_with_geo(self, only_geo_and_text=True):
        fields = {"geo": 1, "text": 1} if only_geo_and_text else {}
        return self.collection.find({"geo": {"$exists": True, "$ne": None}}, fields)

    def get_documents_with_media(self, link=False, user_id=False, created_at=False):
        return_values = {}
        flags = [(link, "extended_entities.media.media_url_https"), (user_id, "user.id"), (created_at, "created_at")]

        for flag, field in flags:
            if flag:
                return_values[field] = 1

        cur = self.collection.find(
            {"extended_entities.media": {"$exists": True, "$not": {"$size": 0}}},
            return_values if return_values else None,
        )
        return cur
