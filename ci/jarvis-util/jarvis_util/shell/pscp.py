"""
This module provides methods to distribute a command among multiple
nodes using SSH. This class is intended to be called from Exec,
not by general users.
"""

from .scp import Scp
from .exec_info import Executable


class Pscp(Executable):
    """
    Execute commands on multiple hosts using SSH.
    """

    def __init__(self, paths, exec_info):
        """
        Copy files to a set of remote hosts via rsync.

        Case 1: Paths is a single file:
        paths = '/tmp/hi.txt'
        '/tmp/hi.txt' will be copied to user@host:/tmp/hi.txt

        Case 2: Paths is a list of files:
        paths = ['/tmp/hi1.txt', '/tmp/hi2.txt']
        Repeat Case 1 twice.

        Case 3: Paths is a list of tuples of files:
        paths = [('/tmp/hi.txt', '/tmp/remote_hi.txt')]
        '/tmp/hi.txt' will be copied to user@host:'/tmp/remote_hi.txt'

        :param paths: Either a path to a file, a list of files, or a list of
        tuples of files.
        :param exec_info: Connection information for SSH
        """
        super().__init__()
        self.exec_async = exec_info.exec_async
        self.hosts = exec_info.hostfile.hosts
        self.scp_nodes = []
        self.stdout = {}
        self.stderr = {}
        self.hosts = exec_info.hostfile.hosts
        for host in self.hosts:
            ssh_exec_info = exec_info.mod(hostfile=None,
                                          hosts=host,
                                          exec_async=True)
            self.scp_nodes.append(Scp(paths, ssh_exec_info))
        if self.exec_async:
            self.wait()

    def wait(self):
        self.wait_list(self.scp_nodes)
        self.per_host_outputs(self.scp_nodes)
        self.set_exit_code()

    def set_exit_code(self):
        self.set_exit_code_list(self.scp_nodes)

