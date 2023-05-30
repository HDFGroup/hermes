"""
Provides methods for executing a program or workflow locally. This class
is intended to be called from Exec, not by general users.
"""

import time
import subprocess
import os
import sys
import io
import threading
from jarvis_util.jutil_manager import JutilManager
from .exec_info import ExecInfo, ExecType, Executable


class LocalExec(Executable):
    """
    Provides methods for executing a program or workflow locally.
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a program or workflow

        :param cmd: list of commands or a single command string
        :param exec_info: Info needed to execute processes locally
        """

        super().__init__()
        jutil = JutilManager.get_instance()
        cmd = self.smash_cmd(cmd)

        # Managing console output and collection
        self.collect_output = exec_info.collect_output
        self.pipe_stdout = exec_info.pipe_stdout
        self.pipe_stderr = exec_info.pipe_stderr
        self.pipe_stdout_fp = None
        self.pipe_stderr_fp = None
        self.hide_output = exec_info.hide_output
        # pylint: disable=R1732
        if self.collect_output is None:
            self.collect_output = jutil.collect_output
        if self.hide_output is None:
            self.hide_output = jutil.hide_output
        # pylint: enable=R1732
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()
        self.last_stdout_size = 0
        self.last_stderr_size = 0
        self.executing_ = True
        self.exit_code = 0

        # Copy ENV
        self.env = exec_info.env.copy()
        for key, val in os.environ.items():
            if key not in self.env:
                self.env[key] = val

        # Managing command execution
        self.cmd = cmd
        self.sudo = exec_info.sudo
        self.stdin = exec_info.stdin
        self.exec_async = exec_info.exec_async
        self.sleep_ms = exec_info.sleep_ms
        if exec_info.cwd is None:
            self.cwd = os.getcwd()
        else:
            self.cwd = exec_info.cwd
        if jutil.debug_local_exec:
            print(cmd)
        self._start_bash_processes()

    def _start_bash_processes(self):
        if self.sudo:
            self.cmd = f'sudo {self.cmd}'
        time.sleep(self.sleep_ms)
        # pylint: disable=R1732
        self.proc = subprocess.Popen(self.cmd,
                                     cwd=self.cwd,
                                     env=self.env,
                                     shell=True)
        # pylint: enable=R1732
        if not self.exec_async:
            self.wait()

    def wait(self):
        self.proc.wait()
        self.set_exit_code()
        return self.exit_code

    def set_exit_code(self):
        self.exit_code = self.proc.returncode

    def get_pid(self):
        if self.proc is not None:
            return self.proc.pid
        else:
            return None


class LocalExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.LOCAL, **kwargs)
