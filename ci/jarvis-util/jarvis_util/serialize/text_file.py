import configparser
from jarvis_util.serialize.serializer import Serializer

class TextFile(Serializer):
    def __init__(self, path):
        self.path = path

    def load(self):
        with open(self.path) as fp:
            data = fp.read()
        return data

    def save(self, data):
        with open(self.path, 'w') as fp:
            fp.write(data)