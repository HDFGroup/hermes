from abc import ABC,abstractmethod

class Serializer(ABC):
    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def save(self, data):
        pass