import psycopg2
from psycopg2.extras import Json
from pymongo import MongoClient


class DBHelperPostgres:
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 5432,
                 user: str = 'postgres',
                 password: str = None,
                 database: str = 'mydb',
                 table: str = 'tweets',
                 ):
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.table = table
        self.conn: psycopg2.extensions.connection = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port)

        self.cur: psycopg2.extensions.cursor = self.conn.cursor()

    def insert_tweet(self, tweet: dict) -> None:
        table_name = self.table

        item = dict(tweet_id=tweet['id'],
                    tweet_json=Json(tweet),
                    tweet_text=tweet['text'],
                    tweet_user_id=tweet['user']['id'],
                    )

        sql = ("INSERT INTO %s (%s) "
               "VALUES (%%(%s)s );") % (table_name, ', '.join(item), ')s, %('.join(item))

        self.cur.execute(sql)
        self.conn.commit()

    def is_connected(self):
        raise NotImplementedError


class DBHelperMongo:
    def __init__(self, 
                 mongodb_url: str = 'mongodb://127.0.0.1:27017',
                 database: str = 'mydb',
                 table: str = 'tweets'
                 ):

        self.client = MongoClient(mongodb_url)
        self.db = self.client[database]
        self.collection = self.db[table]

    @property
    def conn(self):
        return self.client

    def insert_tweet(self, tweet: dict):
        tweet['_id'] = tweet['id']  # a default MongoDB index
        return self.collection.insert_one(tweet)

    def find(self, tweet_id: int):
        return self.collection.find({'_id': tweet_id})
