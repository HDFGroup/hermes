from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo


class PosixTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.POSIX_CMD = f"{self.CMAKE_BINARY_DIR}/bin/posix_adapter_test"
        self.HERMES_POSIX_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_posix_adapter_test"
        self.POSIX_MPI_CMD = f"{self.CMAKE_BINARY_DIR}/bin/posix_adapter_mpi_test"
        self.HERMES_POSIX_MPI_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_posix_adapter_test_mpi"
        self.POSIX_SIMPLE_IO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/posix_simple_io"
        self.HERMES_POSIX_SIMPLE_IO_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_posix_simple_io"

    def test_posix_basic(self):
        posix_cmd = f"{self.POSIX_CMD}"
        node = Exec(posix_cmd)
        return node.exit_code

    def test_hermes_posix_basic_small(self):
        posix_cmd = f"{self.HERMES_POSIX_CMD} " \
                    f"~[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        print(f"CMD: {posix_cmd}")
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_posix_basic_large(self):
        posix_cmd = f"{self.HERMES_POSIX_CMD} " \
                    f"[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_posix_basic_mpi(self):
        posix_cmd = f"{self.POSIX_MPI_CMD}"
        spawn_info = self.spawn_info(nprocs=2)
        node = Exec(posix_cmd, spawn_info)
        return node.exit_code

    def test_hermes_posix_basic_mpi_small(self):
        posix_cmd = f"{self.HERMES_POSIX_CMD} " \
                    f"~[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_posix_basic_mpi_large(self):
        posix_cmd = f"{self.HERMES_POSIX_CMD} " \
                    f"[request_size=range-large]  " \
                    f"--reporter compact -d yes"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_posix_simple_io_omp_default(self):
        posix_cmd = f"{self.HERMES_POSIX_SIMPLE_IO_CMD} " \
                    f"/tmp/test_hermes/hi.txt 0 1024 8 0"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kDefault')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_posix_simple_io_omp_scratch(self):
        posix_cmd = f"{self.HERMES_POSIX_SIMPLE_IO_CMD} " \
                    f"/tmp/test_hermes/hi.txt 0 1024 8 0"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kScratch')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_posix_simple_io_omp_preload(self):
        posix_cmd = f"{self.HERMES_POSIX_SIMPLE_IO_CMD} " \
                    f"/tmp/test_hermes/hi.txt 0 1024 8 0"
        spawn_info = self.spawn_info(nprocs=2,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kScratch',
                                     api='posix')
        self.start_daemon(spawn_info)
        node = Exec(posix_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code
