"""
This module provides methods to execute a single command remotely using SSH.
This class is intended to be called from Exec, not by general users.
"""
from .local_exec import LocalExec
from .exec_info import ExecInfo, ExecType


class SshExec(LocalExec):
    """
    This class provides methods to execute a command via SSH.
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a command remotely via SSH

        :param cmd: A list of commands or a single command string
        :param exec_info: Info needed to execute command with SSH
        """

        cmd = self.smash_cmd(cmd)
        self.addr = exec_info.hostfile.hosts[0]
        self.user = exec_info.user
        self.pkey = exec_info.pkey
        self.port = exec_info.port
        self.sudo = exec_info.sudo
        self.ssh_env = exec_info.env
        super().__init__(self.ssh_cmd(cmd),
                         exec_info.mod(env=exec_info.basic_env))

    def ssh_cmd(self, cmd):
        lines = ['ssh']
        if self.pkey is not None:
            lines.append(f'-i {self.pkey}')
        if self.port is not None:
            lines.append(f'-p {self.port}')
        if self.user is not None:
            lines.append(f'{self.user}@{self.addr}')
        else:
            lines.append(f'{self.addr}')
        ssh_cmd = ' '.join(lines)

        cmd_lines = []
        if self.ssh_env is not None:
            for key, val in self.ssh_env.items():
                cmd_lines.append(f'{key}={val}')
        cmd_lines.append(cmd)
        env_cmd = ' '.join(cmd_lines)
        real_cmd = f'{ssh_cmd} \"{env_cmd}\"'
        return real_cmd


class SshExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.SSH, **kwargs)
