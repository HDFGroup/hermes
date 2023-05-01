from jarvis_util.serialize.serializer import Serializer
import yaml


class YamlFile(Serializer):
    def __init__(self, path):
        self.path = path

    def load(self):
        with open(self.path, 'r') as fp:
            return yaml.load(fp, Loader=yaml.FullLoader)
        return None

    def save(self, data):
        with open(self.path, 'w') as fp:
            yaml.dump(data, fp)
