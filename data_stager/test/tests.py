from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.util.size_conv import SizeConv
import os


class DataStagerTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.TEST_STAGE_IN_CMD = f"{self.CMAKE_BINARY_DIR}/bin/stage_in"

    def test_data_stager_posix_large_aligned(self):
        path = '/tmp/test_hermes/test_data_stager_posix_large_aligned.bin'
        self._make_file(path, '128m')
        spawn_info = self.spawn_info(nprocs=4,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(f"{self.TEST_STAGE_IN_CMD} {path} 0 128m kMinimizeIoTime",
                    spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_data_stager_posix_large_unaligned(self):
        path = '/tmp/test_hermes/test_data_stager_posix_large_unaligned.bin'
        self._make_file(path, '128m')
        spawn_info = self.spawn_info(nprocs=4,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(f"{self.TEST_STAGE_IN_CMD} {path} .5m 127.5m "
                    f"kMinimizeIoTime",
                    spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_data_stager_posix_small_aligned(self):
        path = '/tmp/test_hermes/test_data_stager_posix_small_aligned.bin'
        self._make_file(path, '64m')
        spawn_info = self.spawn_info(nprocs=4,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(f"{self.TEST_STAGE_IN_CMD} {path} 0 64m kMinimizeIoTime",
                    spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_data_stager_posix_small_misaligned(self):
        path = '/tmp/test_hermes/test_data_stager_posix_small_misaligned.bin'
        self._make_file(path, '64m')
        spawn_info = self.spawn_info(nprocs=4,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(f"{self.TEST_STAGE_IN_CMD} {path} .5m 63.5m kMinimizeIoTime",
                    spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_data_stager_directory(self):
        os.makedirs('/tmp/test_hermes/a', exist_ok=True)
        path = '/tmp/test_hermes/a/test_data_stager_posix_small_misaligned.bin'
        self._make_file(path + "0", '64m')
        self._make_file(path + "1", '128m')
        self._make_file(path + "2", '4k')
        self._make_file(path + "3", '16k')
        spawn_info = self.spawn_info(nprocs=4,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(f"{self.TEST_STAGE_IN_CMD} {path} 0 0 kMinimizeIoTime",
                    spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def _make_file(self, path, size):
        size = SizeConv.to_int(size)
        with open(path, 'w') as fp:
            fp.write("\0" * size)