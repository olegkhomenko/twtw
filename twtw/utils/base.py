from io import BytesIO

import requests
import yaml
from PIL import Image


def load_config(path: str):
    with open(path, "r") as fin:
        config = yaml.load(fin, Loader=yaml.FullLoader)

    return config


def image_from_url(url: str):
    response = requests.get(url)
    return Image.open(BytesIO(response.content))
