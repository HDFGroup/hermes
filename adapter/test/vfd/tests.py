from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo


class VfdTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.VFD_CMD = f"{self.CMAKE_BINARY_DIR}/bin/vfd_adapter_test"
        self.HERMES_VFD_CMD = f"{self.CMAKE_BINARY_DIR}/bin/hermes_vfd_adapter_test"
        self.disable_testing = True

    def test_vfd_basic(self):
        vfd_cmd = f"{self.VFD_CMD}"
        node = Exec(vfd_cmd)
        return node.exit_code

    def test_hermes_vfd_default(self):
        vfd_cmd = f"{self.HERMES_VFD_CMD} SingleWrite"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kDefault',
                                     api='vfd')
        self.start_daemon(spawn_info)
        node = Exec(vfd_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code

    def test_hermes_vfd_scratch(self):
        vfd_cmd = f"{self.HERMES_VFD_CMD} [scratch]"
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server',
                                     hermes_mode='kScratch',
                                     api='vfd')
        self.start_daemon(spawn_info)
        node = Exec(vfd_cmd, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code