from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo


class MpiioTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.MPIIO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/mpiio_adapter_test"
        self.HERMES_MPIIO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_mpiio_adapter_test"

        self.disable_testing = False

    def test_hermes_mpiio_basic_sync(self):
        mpiio_cmd = f"{self.HERMES_MPIIO_CMD} " \
                    f"[synchronicity=sync]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(mpiio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_mpiio_basic_async(self):
        mpiio_cmd = f"{self.HERMES_MPIIO_CMD} " \
                    f"[synchronicity=async]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(mpiio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code