import psutil
from .exec import Exec


class Rm(Exec):
    def __init__(self, path, exec_info):
        super().__init__(f"rm -rf {path}", exec_info)
