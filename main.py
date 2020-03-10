import argparse
from twtw.utils import load_config
from dataclasses import dataclass


@dataclass
class Constants:
    task: str = 'listen-stream'
    tasks: tuple = ('listen-stream', )
    config_path: str = './config.yaml'


def main(args):
    load_config(args.config_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', default=Constants.config_path, help='Path to config file')
    parser.add_argument('--task', default=Constants.task, choices=Constants.tasks)

    args = parser.parse_args()
    main(args)
