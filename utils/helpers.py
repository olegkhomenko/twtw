import asyncio
import os
import string
import time
from typing import Tuple, Union

import gensim
import nltk
import pandas as pd
import twitter
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer, sent_tokenize, word_tokenize

from utils import load_config


def get_friends(user_id: int,
                api: twitter.api.Api,
                n_friends: int = None,
                max_requests=100,
                ) -> list:

    friends = []
    next_cursor = -1
    n_requests = 0

    if n_friends is None:
        user = api.GetUser(user_id=user_id)
        n_friends = user.friends_count

    while len(friends) < n_friends and n_requests < max_requests:
        next_cursor, _, curr_friends = api.GetFriendsPaged(user_id, cursor=next_cursor)
        friends += curr_friends
        n_requests += 1

    if n_requests < max_requests:
        assert n_friends == len(friends), f'{n_friends} !- {len(friends)}'

    return friends


def get_statuses(user_id: int,
                 api: twitter.api.Api
                 ):

    pass


def remove_outliers(df: pd.DataFrame):
    mask = df.sub(df.mean()).div(df.std()).abs().le(3).values.reshape(-1)
    return df[mask]


class Timer:    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start
        print(self.interval)


class TWAnalytics:
    FILE_SYNONYMS = 'data/synonyms.yaml'
    FILE_LOCATIONS = 'data/locations.yaml'
    FILE_STOP_WORDS = 'data/stop_words.yaml'
    CONFIG = 'config.yaml'

    def __init__(self, api: twitter.api.Api = None, config_path=None):
        if api is None:
            config_path = config_path if config_path is not None else self.CONFIG
            api_kwargs = load_config(config_path)['twitter_api']
            api = twitter.Api(**api_kwargs)

        self.api = api
        self.check_nltk()
        self.tweet_tokenizer = TweetTokenizer()

    def stream_listener(self, location: str = 'moscow', max_num_of_tweets: int = 10):
        loc = [str(x) for x in self.locations[location]]
        loc_from = ",".join(loc[:2])
        loc_to = ",".join(loc[2:])
        stream = self.api.GetStreamFilter(locations=[loc_from, loc_to])
        results = []
        print("Starting")
        while len(results) < max_num_of_tweets:
            with Timer():
                tweet = next(stream)
                results += [tweet]
                print("Downloaded")
        return results

    def get_friends_df(self, user_id: int):
        return self.friends_to_df(get_friends(user_id, self.api))

    def friends_cities(self,
                       user_id: int = None,
                       friends_df: pd.DataFrame = None,
                       return_friends_df=True,
                       ) -> Union[dict, Tuple[dict, pd.DataFrame]]:

        if user_id is None and friends_df is None:
            raise ValueError("Please pass user_id or friends_df as a parameter")

        if friends_df is None:
            friends_df = self.get_friends_df(user_id)
            if return_friends_df:
                friends_df_orig = friends_df.copy()

        friends_df.location = friends_df.location.map(  # Replace different names of cities w/ a common name
            lambda x: x.split(',')[0] if isinstance(x, str) else x).replace(self.cities)

        friends_cities: pd.Series = friends_df.location.value_counts().sort_values(ascending=False)

        if return_friends_df:
            return friends_cities.to_dict(), friends_df_orig
        return friends_cities.to_dict()

    def friends_followers_count_stats(self,
                                      user_id: int = None,
                                      friends_df: pd.DataFrame = None,
                                      ):

        if user_id is None and friends_df is None:
            raise ValueError("Please pass user_id or friends_df as a parameter")

        if friends_df is None:
            friends_df = self.get_friends_df(user_id)

        return friends_df.followers_count.agg(['min', 'max', 'median', 'std', 'mean']).to_dict()

    def get_tokenized_timeline(self,
                               user_id: int = None,
                               ):
        tweets = self.api.GetUserTimeline(user_id=user_id)
        tweets_sentences = [sent_tokenize(t.full_text, language='russian') for t in tweets]

        tokenized_tweets = []
        for tweet in tweets_sentences:
            tweet_tokenized = []
            for sent in tweet:
                tweet_tokenized += [self._tokenize(sent)]
            tokenized_tweets += [tweet_tokenized]

        return tokenized_tweets

    def _tokenize(self, text: str):
        tokens = self.tweet_tokenizer.tokenize(text)
        tokens = [i for i in tokens if (i not in string.punctuation)]
        stop_words = stopwords.words('russian')
        stop_words.extend(self.stop_words)
        tokens = [i for i in tokens if (i not in stop_words)]
        return tokens

    @staticmethod
    def friends_to_df(friends: list) -> pd.DataFrame:
        return pd.DataFrame({f.id: f.AsDict() for f in friends}).T

    @property
    def cities(self):
        if not hasattr(self, '_synonyms'):
            self._synonyms = load_config(self.FILE_SYNONYMS)
        return self._synonyms['cities']

    @property
    def locations(self):
        if not hasattr(self, '_locations'):
            self._locations = load_config(self.FILE_LOCATIONS)

        return self._locations

    @property
    def stop_words(self):
        if not hasattr(self, '_stop_words'):
            self._stop_words = load_config(self.FILE_STOP_WORDS)

        return self._stop_words

    @classmethod
    def check_nltk(cls):
        for el in ['tokenizers/punkt', 'corpora/stopwords']:
            try:
                nltk.data.find(el)
            except LookupError:
                nltk.download(os.path.basename(el))


class TWModel:
    MODEL_PATH = './182.zip'
    MODEL_URI = 'http://vectors.nlpl.eu/repository/20/182.zip'

    def __init__(self, model_path=None):
        model_path = self.MODEL_PATH if model_path is None else model_path
        self.model = gensim.models.Word2Vec.load(model_path)

    def train(self, sentences: list):
        raise NotImplementedError

    def _download_weights(self):
        # TODO: Download using model uri
        raise NotImplementedError
