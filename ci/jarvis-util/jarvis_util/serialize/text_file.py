"""
This module stores data into a file in a human-readable way
"""
from jarvis_util.serialize.serializer import Serializer


class TextFile(Serializer):
    """
    This class stores data directly into a file using str() as the
    serialization method. The data is intended to be human-readable.
    """
    def __init__(self, path):
        self.path = path

    def load(self):
        with open(self.path, 'r', encoding='utf-8') as fp:
            data = fp.read()
        return data

    def save(self, data):
        with open(self.path, 'w', encoding='utf-8') as fp:
            fp.write(data)
