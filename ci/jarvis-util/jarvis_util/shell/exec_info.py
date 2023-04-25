from enum import Enum
from jarvis_util.util.hostfile import Hostfile
import copy
import os
from abc import ABC, abstractmethod


class ExecType(Enum):
    LOCAL = 'LOCAL'
    SSH = 'SSH'
    PSSH = 'PSSH'
    MPI = 'MPI'


class ExecInfo:
    def __init__(self,  exec_type=ExecType.LOCAL, nprocs=None, ppn=None,
                 user=None, pkey=None, port=None, hostfile=None, env=None,
                 sleep_ms=0, sudo=False, cwd=None, hosts=None,
                 collect_output=None, pipe_stdout=None, pipe_stderr=None,
                 hide_output=None, exec_async=False, stdin=None):
        self.exec_type = exec_type
        self.nprocs = nprocs
        self.user = user
        self.pkey = pkey
        self.port = port
        self.ppn = ppn
        self.hosts = hosts
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

    def _set_env(self, env):
        if env is None:
            self.env = {}
        basic_env = [
            'PATH', 'LD_LIBRARY_PATH', 'LIBRARY_PATH', 'CMAKE_PREFIX_PATH',
            'PYTHON_PATH', 'CPATH', 'INCLUDE'
        ]
        for key in basic_env:
            if key not in os.environ:
                continue
            self.basic_env[key] = os.getenv(key)
        for key, val in self.basic_env.items():
            if key not in self.env:
                self.env[key] = val

    def _set_hostfile(self, hostfile=None, hosts=None):
        if hostfile is not None:
            if isinstance(hostfile, str):
                self.hostfile = Hostfile(hostfile=hostfile)
            elif isinstance(hostfile, Hostfile):
                self.hostfile = hostfile
            else:
                raise Exception("Hostfile is neither string nor Hostfile")
        if hosts is not None:
            if isinstance(hosts, list):
                self.hostfile = Hostfile(all_hosts=hosts)
            elif isinstance(hosts, str):
                self.hostfile = Hostfile(all_hosts=[hosts])
            elif isinstance(hosts, Hostfile):
                self.hostfile = hosts
            else:
                raise Exception("Host set is neither str, list or Hostfile")

        if hosts is not None and hostfile is not None:
            raise Exception("Must choose either hosts or hostfile, not both")

        if self.hostfile is None:
            self.hostfile = Hostfile()

    def mod(self, **kwargs):
        keys = ['exec_type', 'nprocs', 'ppn', 'user', 'pkey', 'port',
                'hostfile', 'env', 'sleep_ms', 'sudo',
                'cwd', 'hosts', 'collect_output',
                'pipe_stdout', 'pipe_stderr', 'hide_output',
                'exec_async', 'stdin']
        for key in keys:
            if key not in kwargs:
                kwargs[key] = getattr(self, key)
        return ExecInfo(**kwargs)

    def copy(self):
        return self.mod()


class Executable:
    def __init__(self):
        self.exit_code = None
        self.stdout = ""
        self.stderr = ""

    @abstractmethod
    def set_exit_code(self):
        pass

    @abstractmethod
    def wait(self):
        pass

