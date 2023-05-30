"""
This module provides methods to execute a single command remotely using SSH.
This class is intended to be called from Exec, not by general users.
"""
from .local_exec import LocalExec
from .exec_info import Executable


class _Scp(LocalExec):
    """
    This class provides methods to copy data over SSH using the "rsync"
    command utility in Linux
    """

    def __init__(self, src_path, dst_path, exec_info):
        """
        Copy a file or directory from source to destination via rsync

        :param src_path: The path to the file on the host
        :param dst_path: The desired file path on the remote host
        :param exec_info: Info needed to execute command with SSH
        """

        self.addr = exec_info.hostfile.hosts[0]
        self.src_path = src_path
        self.dst_path = dst_path
        self.user = exec_info.user
        self.pkey = exec_info.pkey
        self.port = exec_info.port
        self.sudo = exec_info.sudo
        super().__init__(self.rsync_cmd(src_path, dst_path),
                         exec_info.mod(env=exec_info.basic_env))

    def rsync_cmd(self, src_path, dst_path):
        lines = ['rsync -ha']
        if self.pkey is not None or self.port is not None:
            ssh_lines = ['ssh']
            if self.pkey is not None:
                ssh_lines.append(f'-i {self.pkey}')
            if self.port is not None:
                ssh_lines.append(f'-p {self.port}')
            ssh_cmd = ' '.join(ssh_lines)
            lines.append(f'-e \'{ssh_cmd}\'')
        lines.append(src_path)
        if self.user is not None:
            lines.append(f'{self.user}@{self.addr}:{dst_path}')
        else:
            lines.append(f'{self.addr}:{dst_path}')
        rsync_cmd = ' '.join(lines)
        return rsync_cmd


class Scp(Executable):
    """
    Secure copy data between two hosts.
    """

    def __init__(self, paths, exec_info):
        """
        Copy files via rsync.

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
        self.paths = paths
        self.exec_info = exec_info
        self.scp_nodes = []
        if isinstance(paths, str):
            self._exec_single_path(paths)
        if isinstance(paths, list):
            if len(paths) == 0:
                raise Exception('Must have at least one path to scp')
            elif isinstance(paths[0], str):
                self._exec_many_paths(paths)
            elif isinstance(paths[0], tuple):
                self._exec_many_paths_tuple(paths)
            elif isinstance(paths[0], list):
                self._exec_many_paths_tuple(paths)
        if not self.exec_info.exec_async:
            self.wait()

    def _exec_single_path(self, path):
        self.scp_nodes.append(_Scp(path, path, self.exec_info))

    def _exec_many_paths(self, paths):
        for path in paths:
            self.scp_nodes.append(_Scp(path, path, self.exec_info))

    def _exec_many_paths_tuple(self, path_tlist):
        for src, dst in path_tlist:
            self.scp_nodes.append(_Scp(src, dst, self.exec_info))

    def wait(self):
        self.wait_list(self.scp_nodes)
        self.smash_list_outputs(self.scp_nodes)
        self.set_exit_code()
        return self.exit_code

    def set_exit_code(self):
        self.set_exit_code_list(self.scp_nodes)
