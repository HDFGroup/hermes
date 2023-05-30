"""
This module contains methods to serialize and deserialize data from
a pickle file.
"""

import pickle as pkl
from jarvis_util.serialize.serializer import Serializer


class PickleFile(Serializer):
    """
    This class serializes and deserializes data from a pickle file
    """

    def __init__(self, path):
        self.path = path

    def load(self):
        with open(self.path, 'rb') as fp:
            return pkl.load(fp)

    def save(self, data):
        with open(self.path, 'wb') as fp:
            pkl.dump(data, fp)
