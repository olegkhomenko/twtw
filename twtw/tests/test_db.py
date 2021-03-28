import unittest
from twtw.utils.db import DBHelperMongo
from twtw.utils.helpers import TWAnalytics


class MongoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.twa = TWAnalytics()
        self.mongo = DBHelperMongo()

    def test_stream_and_insert(self):
        result = self.twa.stream_listener(max_num_of_tweets=1)
        self.assertTrue(len(result) == 1)
        for tw in result:
            self.mongo.insert_tweet(tw)

        for tw in self.mongo.find(result[0]["_id"]):
            self.assertEqual(tw["_id"], result[0]["_id"])
