import twitter
import pandas as pd


def get_friends(user_id: int,
                api: twitter.api.Api,
                n_friends: int = None,
                max_requests=100,
                ):

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


class Statistics:
    def __init__(self, ):
        pass

    @staticmethod
    def friends_statistics(friends: list):
        pass

class Analytics:
    @staticmethod
    def favorit