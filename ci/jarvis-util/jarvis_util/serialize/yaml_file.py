"""
This module contains methods to serialize and deserialize data from
a human-readable YAML file.
"""
from jarvis_util.serialize.serializer import Serializer
import yaml


class YamlFile(Serializer):
    """
    This class contains methods to serialize and deserialize data from
    a human-readable YAML file.
    """
    def __init__(self, path):
        self.path = path

    def load(self):
        with open(self.path, 'r', encoding='utf-8') as fp:
            return yaml.load(fp, Loader=yaml.FullLoader)
        return None

    def save(self, data):
        with open(self.path, 'w', encoding='utf-8') as fp:
            yaml.dump(data, fp)
