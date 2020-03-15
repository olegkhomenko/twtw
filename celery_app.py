import argparse
import pprint
import time

import twitter
from celery import Celery

from twtw.utils.db import DBHelperMongo
from twtw.utils.helpers import TWAnalytics, load_config
from easydict import EasyDict as edict

app = Celery('worker', broker='pyamqp://guest@localhost//')


@app.task
def listen_stream(args=None, config='./configs/listen_stream.yaml'):
    if args is None:
        args = edict()
        config = load_config(config)
        for k, v in config.items():
            args[k] = v

    elif args.config is not None:
        print(args.config)
        config = load_config(args.config)
        for k, v in config.items():
            setattr(args, k, v)

    print(args)

    twa = TWAnalytics()
    database = DBHelperMongo()
    result = twa.stream_listener(max_num_of_tweets=100)
    for tweet in result:
        print("Inserting tweet: ")
        database.insert_tweet(tweet)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, help='If config_path is provided, args will be overwritten using config file')
    parser.add_argument('--city', default='moscow', help='City to listen stream')
    parser.add_argument('--db', default='mongo', choices=['mongo', 'postgres'], help='Type of DB to be used to store results')
    args = parser.parse_args()
    raise NotImplementedError("Run via celery -A celery_app worker --loglevel=info")

elif __name__ == 'celery_app':
    task_id = uuid()
    listen_stream.apply_async(task_id=task_id)
    async_result = AsyncResult(id=task_id)
