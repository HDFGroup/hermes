"""
This module contains methods to serialize and deserialize data from
a human-readable ini file.
"""
import configparser
from jarvis_util.serialize.serializer import Serializer


class IniFile(Serializer):
    """
    This class contains methods to serialize and deserialize data from
    a human-readable ini file.
    """
    def __init__(self, path):
        self.path = path

    def load(self):
        config = configparser.ConfigParser()
        config.read(self.path)
        return config

    def save(self, data):
        with open(self.path, 'w', encoding='utf-8') as fp:
            data.write(fp)
