from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo


class NativeTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.TEST_BUCKET_CMD = f"{self.CMAKE_BINARY_DIR}/bin/test_bucket"
        self.TEST_BUFFER_POOL_CMD = f"{self.CMAKE_BINARY_DIR}/bin/test_buffer_pool"
        self.TEST_TRAIT_CMD = f"{self.CMAKE_BINARY_DIR}/bin/test_trait"
        self.TEST_TAG_CMD = f"{self.CMAKE_BINARY_DIR}/bin/test_tag"
        self.TEST_BINLOG_CMD = f"{self.CMAKE_BINARY_DIR}/bin/test_binlog"
        self.TEST_MULTINODE_PUT_GET_CMD = f"{self.CMAKE_BINARY_DIR}/bin/test_multinode_put_get"

    def test_bucket(self):
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(self.TEST_BUCKET_CMD, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_buffer_pool(self):
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(self.TEST_BUFFER_POOL_CMD, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_trait(self):
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(self.TEST_TRAIT_CMD, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_tag(self):
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(self.TEST_TAG_CMD, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_binlog(self):
        node = Exec(self.TEST_BINLOG_CMD)

    def test_multinode_put_get(self):
        spawn_info = self.spawn_info(nprocs=2,
                                     ppn=1,
                                     hostfile=None,  # TODO(llogan)
                                     hermes_conf='hermes_server')
        self.start_daemon(spawn_info)
        node = Exec(self.TEST_MULTINODE_PUT_GET_CMD, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code
