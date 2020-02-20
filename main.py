import twitter
import yaml
import requests
import argparse
from io import BytesIO
from PIL import Image
from utils import load_config


def main(args):
    load_config(args.config_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path')

    args = parser.parse_args()
    main(args)
