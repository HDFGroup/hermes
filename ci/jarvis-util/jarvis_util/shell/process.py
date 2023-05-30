"""
This module provides various wrappers for methods which manage processes
in the cluster. Examples include killing processes, determining whether
or not a process exists, etc.
"""

from .exec import Exec


class Kill(Exec):
    """
    Kill all processes which match the name regex.
    """

    def __init__(self, cmd, exec_info):
        """
        Kill all processes which match the name regex.

        :param cmd: A regex for the command to kill
        :param exec_info: Info needed to execute the command
        """
        super().__init__(f"pkill {cmd}", exec_info)
