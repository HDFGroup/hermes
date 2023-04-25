from .local_exec import LocalExec
from .pssh_exec import PsshExec
from .pssh_exec import SshExec
from .mpi_exec import MpiExec
from .exec_info import ExecInfo, ExecType, Executable


class Exec(Executable):
    def __init__(self, cmd, exec_info=None):
        super().__init__()
        if exec_info is None:
            exec_info = ExecInfo()
        if exec_info.exec_type == ExecType.LOCAL:
            self.exec_ = LocalExec(cmd, exec_info)
        elif exec_info.exec_type == ExecType.SSH:
            self.exec_ = SshExec(cmd, exec_info)
        elif exec_info.exec_type == ExecType.PSSH:
            self.exec_ = PsshExec(cmd, exec_info)
        elif exec_info.exec_type == ExecType.MPI:
            self.exec_ = MpiExec(cmd, exec_info)
        self.set_exit_code()
        self.set_output()

    def wait(self):
        self.exec_.wait()
        self.set_output()
        self.set_exit_code()
        return self.exit_code

    def set_output(self):
        self.stdout = self.exec_.stdout
        self.stderr = self.exec_.stderr

    def set_exit_code(self):
        self.exec_.set_exit_code()
        self.exit_code = self.exec_.exit_code
