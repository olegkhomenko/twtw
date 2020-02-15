from typing import Union, Tuple

import pandas as pd
import twitter

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


class TWAnalytics:
    FILE_SYNONYMS = 'data/synonyms.yaml'

    def __init__(self, api: twitter.api.Api):
        self.api = api

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

    @staticmethod
    def friends_to_df(friends: list) -> pd.DataFrame:
        return pd.DataFrame({f.id: f.AsDict() for f in friends}).T

    @property
    def cities(self):
        if not hasattr(self, '_synonyms'):
            self._synonyms = load_config(self.FILE_SYNONYMS)
        return self._synonyms['cities']
