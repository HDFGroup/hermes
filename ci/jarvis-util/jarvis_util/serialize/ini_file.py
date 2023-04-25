import configparser
from jarvis_util.serialize.serializer import Serializer

class IniFile(Serializer):
    def __init__(self, path):
        self.path = path

    def load(self):
        config = configparser.ConfigParser()
        config.read(self.path)
        return config

    def save(self, data):
        with open(self.path, 'w') as fp:
            data.write(fp)