"""
This module contains data structures for determining how to execute
a subcommand. This includes information such as storing SSH keys,
passwords, working directory, etc.
"""

from enum import Enum
from jarvis_util.util.hostfile import Hostfile
import os
from abc import ABC, abstractmethod


class ExecType(Enum):
    """
    Different program execution methods.
    """

    LOCAL = 'LOCAL'
    SSH = 'SSH'
    PSSH = 'PSSH'
    MPI = 'MPI'


class ExecInfo:
    """
    Contains all information needed to execute a program. This includes
    parameters such as the path to key-pairs, the hosts to run the program
    on, number of processes, etc.
    """
    def __init__(self,  exec_type=ExecType.LOCAL, nprocs=None, ppn=None,
                 user=None, pkey=None, port=None,
                 hostfile=None, hosts=None, env=None,
                 sleep_ms=0, sudo=False, cwd=None,
                 collect_output=None, pipe_stdout=None, pipe_stderr=None,
                 hide_output=None, exec_async=False, stdin=None):
        """

        :param exec_type: How to execute a program. SSH, MPI, Local, etc.
        :param nprocs: Number of processes to spawn. E.g., MPI uses this
        :param ppn: Number of processes per node. E.g., MPI uses this
        :param user: The user to execute command under. E.g., SSH, PSSH
        :param pkey: The path to the private key. E.g., SSH, PSSH
        :param port: The port to use for connection. E.g., SSH, PSSH
        :param hostfile: The hosts to launch command on. E.g., PSSH, MPI
        :param hosts: A list (or single string) of host names to run command on.
        :param env: The environment variables to use for command.
        :param sleep_ms: Sleep for a period of time AFTER executing
        :param sudo: Execute command with root privilege. E.g., SSH, PSSH
        :param cwd: Set current working directory. E.g., SSH, PSSH
        :param collect_output: Collect program output in python buffer
        :param pipe_stdout: Pipe STDOUT into a file. (path string)
        :param pipe_stderr: Pipe STDERR into a file. (path string)
        :param hide_output: Whether to print output to console
        :param exec_async: Whether to execute program asynchronously
        :param stdin: Any input needed by the program. Only local
        """

        self.exec_type = exec_type
        self.nprocs = nprocs
        self.user = user
        self.pkey = pkey
        self.port = port
        self.ppn = ppn
        self.hostfile = hostfile
        self._set_hostfile(hostfile=hostfile, hosts=hosts)
        self.env = env
        self.basic_env = {}
        self._set_env(env)
        self.cwd = cwd
        self.sudo = sudo
        self.sleep_ms = sleep_ms
        self.collect_output = collect_output
        self.pipe_stdout = pipe_stdout
        self.pipe_stderr = pipe_stderr
        self.hide_output = hide_output
        self.exec_async = exec_async
        self.stdin = stdin
        self.keys = ['exec_type', 'nprocs', 'ppn', 'user', 'pkey', 'port',
                     'hostfile', 'env', 'sleep_ms', 'sudo',
                     'cwd', 'hosts', 'collect_output',
                     'pipe_stdout', 'pipe_stderr', 'hide_output',
                     'exec_async', 'stdin']

    def _set_env(self, env):
        if env is None:
            self.env = {}
        else:
            self.env = env
        basic_env = [
            'PATH', 'LD_LIBRARY_PATH', 'LIBRARY_PATH', 'CMAKE_PREFIX_PATH',
            'PYTHON_PATH', 'CPATH', 'INCLUDE', 'JAVA_HOME'
        ]
        self.basic_env = {}
        for key in basic_env:
            if key not in os.environ:
                continue
            self.basic_env[key] = os.getenv(key)
        for key, val in self.basic_env.items():
            if key not in self.env:
                self.env[key] = val
        self.basic_env.update(self.env)
        if 'LD_PRELOAD' in self.basic_env:
            del self.basic_env['LD_PRELOAD']

    def _set_hostfile(self, hostfile=None, hosts=None):
        if hostfile is not None:
            if isinstance(hostfile, str):
                self.hostfile = Hostfile(hostfile=hostfile)
            elif isinstance(hostfile, Hostfile):
                self.hostfile = hostfile
            else:
                raise Exception('Hostfile is neither string nor Hostfile')
        if hosts is not None:
            if isinstance(hosts, list):
                self.hostfile = Hostfile(all_hosts=hosts)
            elif isinstance(hosts, str):
                self.hostfile = Hostfile(all_hosts=[hosts])
            elif isinstance(hosts, Hostfile):
                self.hostfile = hosts
            else:
                raise Exception('Host set is neither str, list or Hostfile')

        if hosts is not None and hostfile is not None:
            raise Exception('Must choose either hosts or hostfile, not both')

        if self.hostfile is None:
            self.hostfile = Hostfile()

    def mod(self, **kwargs):
        self._mod_kwargs(kwargs)
        return ExecInfo(**kwargs)

    def _mod_kwargs(self, kwargs):
        for key in self.keys:
            if key not in kwargs and hasattr(self, key):
                kwargs[key] = getattr(self, key)

    def copy(self):
        return self.mod()


class Executable(ABC):
    """
    An abstract class representing a class which is intended to run
    shell commands. This includes SSH, MPI, etc.
    """

    def __init__(self):
        self.exit_code = None
        self.stdout = ''
        self.stderr = ''

    def failed(self):
        return self.exit_code != 0

    @abstractmethod
    def set_exit_code(self):
        pass

    @abstractmethod
    def wait(self):
        pass

    def smash_cmd(self, cmds):
        """
        Convert a list of commands into a single command for the shell
        to execute.

        :param cmds: A list of commands or a single command string
        :return:
        """
        if isinstance(cmds, list):
            return ' && '.join(cmds)
        elif isinstance(cmds, str):
            return cmds
        else:
            raise Exception('Command must be either list or string')

    def wait_list(self, nodes):
        for node in nodes:
            node.wait()

    def smash_list_outputs(self, nodes):
        """
        Combine the outputs of a set of nodes into a single output.
        For example, used if executing multiple commands in sequence.

        :param nodes:
        :return:
        """
        self.stdout = '\n'.join([node.stdout for node in nodes])
        self.stderr = '\n'.join([node.stderr for node in nodes])

    def per_host_outputs(self, nodes):
        """
        Convert the outputs of a set of nodes to a per-host dictionary.
        Used if sending commands to multiple hosts

        :param nodes:
        :return:
        """
        self.stdout = {}
        self.stderr = {}
        self.stdout = {node.addr: node.stdout for node in nodes}
        self.stderr = {node.addr: node.stderr for node in nodes}

    def set_exit_code_list(self, nodes):
        """
        Set the exit code from a set of nodes.

        :param nodes: The set of execution nodes that have been executed
        :return:
        """
        for node in nodes:
            if node.exit_code:
                self.exit_code = node.exit_code
