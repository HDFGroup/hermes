"""
This module provides methods to execute a process in parallel using the
Message Passing Interface (MPI). This module assumes MPI is installed
on the system. This class is intended to be called from Exec,
not by general users.
"""

from jarvis_util.jutil_manager import JutilManager
from jarvis_util.shell.local_exec import LocalExec
from .exec_info import ExecInfo, ExecType


class MpiExec(LocalExec):
    """
    This class contains methods for executing a command in parallel
    using MPI.
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a command using MPI

        :param cmd: A command (string) to execute
        :param exec_info: Information needed by MPI
        """

        self.cmd = cmd
        self.nprocs = exec_info.nprocs
        self.ppn = exec_info.ppn
        self.hostfile = exec_info.hostfile
        self.mpi_env = exec_info.env
        super().__init__(self.mpicmd(),
                         exec_info.mod(env=exec_info.basic_env))

    def mpicmd(self):
        params = [f"mpirun -n {self.nprocs}"]
        if self.ppn is not None:
            params.append(f"-ppn {self.ppn}")
        if len(self.hostfile):
            if self.hostfile.is_subset() or self.hostfile.path is None:
                params.append(f"--host {','.join(self.hostfile.hosts)}")
            else:
                params.append(f"--hostfile {self.hostfile.path}")
        params += [f"-genv {key}={val}" for key, val in self.mpi_env.items()]
        params.append(self.cmd)
        cmd = " ".join(params)
        jutil = JutilManager.get_instance()
        if jutil.debug_mpi_exec:
            print(cmd)
        return cmd


class MpiExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.MPI, **kwargs)
