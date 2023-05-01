import psutil
from .exec import Exec


class Kill(Exec):
    def __init__(self, cmd, exec_info):
        super().__init__(f"pkill {cmd}", exec_info)
