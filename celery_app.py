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
    result = twa.stream_listener(max_num_of_tweets=3)
    for tweet in result:
        print("Inserting tweet: ")
        database.insert_tweet(tweet)


def main(args=None):
    print("Starting main")
    result = listen_stream.delay()
    print("Results ready")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default=None, help='If config_path is provided, args will be overwritten using config file')
    parser.add_argument('--city', default='moscow', help='City to listen stream')
    parser.add_argument('--db', default='mongo', choices=['mongo', 'postgres'], help='Type of DB to be used to store results')
    args = parser.parse_args()

    main(args)


elif __name__ == 'celery_app':
    main()
