"""
This module contains an abstract class used to define classes which
serialize data to a file.
"""

from abc import ABC, abstractmethod


class Serializer(ABC):
    """
    An abstract class which loads serialized data from a file and
    saves serialized data to a file.
    """

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def save(self, data):
        pass
