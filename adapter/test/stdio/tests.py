from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo


class StdioTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.STDIO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/stdio_adapter_test"
        self.HERMES_STDIO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_stdio_adapter_test"
        self.STDIO_MPI_CMD = f"{self.CMAKE_BINARY_DIR}/bin/stdio_adapter_test_mpi"
        self.HERMES_STDIO_MPI_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_stdio_adapter_test_mpi"
        self.STDIO_SIMPLE_IO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/stdio_simple_io"
        self.HERMES_STDIO_SIMPLE_IO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_stdio_simple_io"

        self.HERMES_STDIO_MAPPER_CMD = f"{self.CMAKE_BINARY_DIR}/bin/stdio_adapter_mapper_test"
        self.HERMES_STDIO_LOW_BUF_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_stdio_low_buf_adapter_test"
        self.HERMES_STDIO_MODE_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_stdio_adapter_mode_test"

    def test_stdio_basic(self):
        stdio_cmd = f"{self.STDIO_CMD}"
        node = Exec(stdio_cmd)
        return node.exit_code

    def test_hermes_stdio_mapper(self):
        stdio_cmd = f"{self.HERMES_STDIO_MAPPER_CMD} "
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_basic_small(self):
        stdio_cmd = f"{self.HERMES_STDIO_CMD} " \
                    f"~[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=1,
                                    hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_basic_large(self):
        stdio_cmd = f"{self.HERMES_STDIO_CMD} " \
                    f"[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_mpi_small(self):
        stdio_cmd = f"{self.HERMES_STDIO_CMD} " \
                    f"~[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_mpi_large(self):
        stdio_cmd = f"{self.HERMES_STDIO_CMD} " \
                    f"[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        node = Exec(stdio_cmd,
                    self.spawn_info(nprocs=2,
                                    hermes_conf='hermes_server'))
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_low_buf(self):
        stdio_cmd = f"{self.HERMES_STDIO_LOW_BUF_CMD} "
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_bypass(self):
        stdio_cmd = f"{self.HERMES_STDIO_MODE_CMD} [hermes_mode=bypass]"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kBypass')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_default(self):
        stdio_cmd = f"{self.HERMES_STDIO_MODE_CMD} [hermes_mode=persistent]"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kDefault')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_stdio_scratch(self):
        stdio_cmd = f"{self.HERMES_STDIO_MODE_CMD} [hermes_mode=scratch]"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kScratch')
        self.start_daemon(spawn_info)
        node = Exec(stdio_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

