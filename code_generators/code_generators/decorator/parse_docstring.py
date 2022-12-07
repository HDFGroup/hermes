from abc import ABC, abstractmethod
import re


class ParseDocstring(ABC):
    def __init__(self, api):
        self.api = api
        self.doc_str = self.api.doc_str
        if self.doc_str is None:
            self.doc_str = ""
        self.lines = self.doc_str.splitlines()

    def get_param(self, param_name, required=True):
        param_name = f"@{param_name}"
        for line in self.lines:
            if param_name in line:
                line = line[line.find(param_name) + len(param_name):]
                param_val = line.strip()
                return param_val
        if required:
            raise Exception(f"The parameter {param_name} was not found "
                            f"in {self.api.get_scoped_name()} for RPC")

    @abstractmethod
    def parse(self):
        pass