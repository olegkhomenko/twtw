import argparse

import twitter
from celery import Celery, uuid
from celery.result import AsyncResult
from easydict import EasyDict as edict

from twtw.utils.db import DBHelperMongo
from twtw.utils.helpers import TWAnalytics, load_config

app = Celery("worker", broker="pyamqp://guest@localhost")


@app.task
def listen_stream(
    args=None,
    config="./configs/listen_stream.yaml",
    max_num_of_tweets=100,
    max_iterations=20,
):
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
    for i in range(max_iterations):
        print("Listeining stream, iteration #", i)
        result = twa.stream_listener(max_num_of_tweets=max_num_of_tweets)
        for tweet in result:
            print("Inserting tweet: ")
            database.insert_tweet(tweet)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=str, help="If config_path is provided, args will be overwritten using config file"
    )
    parser.add_argument("--city", default="moscow", help="City to listen stream")
    parser.add_argument(
        "--db", default="mongo", choices=["mongo", "postgres"], help="Type of DB to be used to store results"
    )
    args = parser.parse_args()
    raise NotImplementedError("Run via celery -A celery_app worker --loglevel=info")

elif __name__ == "celery_app":
    task_id = uuid()
    listen_stream.apply_async(
        kwargs={"max_num_of_tweets": 100, "max_iterations": 20}, task_id=task_id, queue="twitter_api"
    )
