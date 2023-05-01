from py_hermes_ci.test_manager import TestManager
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo


class JavaWrapperTestManager(TestManager):
    def spawn_all_nodes(self):
        return self.spawn_info()

    def set_paths(self):
        self.KVSTORE_CMD = f"./gradlew test"
        self.disable_testing = False

    def test_hermes_java(self):
        return
        spawn_info = self.spawn_info(nprocs=1,
                                     hermes_conf='hermes_server',
                                     cwd=f"{self.MY_DIR}/java")
        self.start_daemon(spawn_info)
        node = Exec(self.KVSTORE_CMD, spawn_info)
        self.stop_daemon(spawn_info)
        return node.exit_code
