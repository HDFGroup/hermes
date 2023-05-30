from jarvis_util import *
import time
import os, sys
import pathlib
import inspect
from abc import ABC, abstractmethod


class SpawnInfo(MpiExecInfo):
    def __init__(self, nprocs, hermes_conf=None, hermes_mode=None, api=None,
                 **kwargs):
        super().__init__(nprocs=nprocs, **kwargs)
        self.hermes_conf = hermes_conf
        self.hermes_mode = hermes_mode
        self.api = api


class TestManager(ABC):
    """======================================================================"""
    """ Test Case Constructor """
    """======================================================================"""
    def __init__(self, cmake_source_dir, cmake_binary_dir, address_sanitizer):
        """
        Initialize test manager
        """
        jutil = JutilManager.get_instance()
        jutil.collect_output = False
        jutil.hide_output = False
        jutil.debug_mpi_exec = True
        jutil.debug_local_exec = True
        self.MY_DIR = str(pathlib.Path(inspect.getfile(LocalExecInfo)).parent)
        self.CMAKE_SOURCE_DIR = cmake_source_dir
        self.CMAKE_BINARY_DIR = cmake_binary_dir
        self.HERMES_TRAIT_PATH = f"{self.CMAKE_BINARY_DIR}/bin"
        self.HERMES_CLIENT_CONF = f"{self.CMAKE_SOURCE_DIR}/test/data/hermes_client.yaml"
        self.ADDRESS_SANITIZER = address_sanitizer
        self.daemon = None
        self.disable_testing = False

        os.makedirs("/tmp/test_hermes", exist_ok=True)
        self.tests_ = {}
        self.devices = {}
        self.set_devices()
        self.find_tests()

    def spawn_info(self, nprocs=None, ppn=None, hostfile=None,
                   hermes_conf=None, hermes_mode=None, api=None, cwd=None):
        # Whether to deploy hermes
        use_hermes = hermes_mode is not None \
                     or api == 'native' \
                     or hermes_conf is not None

        # Get the hermes configuration path
        if use_hermes:
            if hermes_conf is None:
                hermes_conf = os.path.join(self.CMAKE_SOURCE_DIR,
                                           'test', 'data',
                                           'hermes_server.yaml')
            else:
                hermes_conf = os.path.join(self.CMAKE_SOURCE_DIR,
                                           'test', 'data',
                                           f"{hermes_conf}.yaml")

        # Basic environment variables
        env = {}
        if use_hermes:
            env.update({
                'HERMES_LOG_OUT': "/tmp/hermes_log.txt",
                'HERMES_CONF': hermes_conf,
                'HERMES_CLIENT_CONF': self.HERMES_CLIENT_CONF,
                'HERMES_TRAIT_PATH': self.HERMES_TRAIT_PATH,
            })
            if hostfile:
                env['HERMES_HOSTFILE'] = hostfile.path

        # Hermes interceptor paths
        if use_hermes:
            if api == 'posix':
                env['LD_PRELOAD'] = f"{self.CMAKE_BINARY_DIR}/bin" \
                                    f"/libhermes_posix.so"
            elif api == 'stdio':
                env['LD_PRELOAD'] = f"{self.CMAKE_BINARY_DIR}/bin" \
                                    f"/libhermes_stdio.so"
            elif api == 'mpiio':
                env['LD_PRELOAD'] = f"{self.CMAKE_BINARY_DIR}/bin" \
                                    f"/libhermes_mpiio.so"
            elif api == 'vfd':
                env['HDF5_PLUGIN_PATH'] = f"{self.CMAKE_BINARY_DIR}/bin"
                env['HDF5_DRIVER'] = 'hdf5_hermes_vfd'

        # Get libasan path
        # Assumes GCC for now
        # TODO(llogan): check if ADDRESS_SANITIZER is enabled...
        if 'LD_PRELOAD' in env:
            node = Exec('gcc -print-file-name=libasan.so',
                        LocalExecInfo(collect_output=True, hide_output=True))
            env['LD_PRELOAD'] = f"{node.stdout.strip()}:{env['LD_PRELOAD']}"

        # Hermes mode
        if hermes_mode == 'kDefault':
            env['HERMES_ADAPTER_MODE'] = 'kDefault'
        if hermes_mode == 'kScratch':
            env['HERMES_ADAPTER_MODE'] = 'kScratch'
        if hermes_mode == 'kBypass':
            env['HERMES_ADAPTER_MODE'] = 'kBypass'

        return SpawnInfo(nprocs=nprocs,
                         ppn=ppn,
                         hostfile=hostfile,
                         hermes_conf=hermes_conf,
                         hermes_mode=hermes_mode,
                         api=api,
                         env=env,
                         cwd=cwd)

    @abstractmethod
    def set_paths(self):
        pass

    def set_devices(self):
        self.devices['nvme'] = '/tmp/test_hermes/'
        self.devices['ssd'] = '/tmp/test_hermes/'
        self.devices['hdd'] = '/tmp/test_hermes/'
        self.devices['pfs'] = '/tmp/test_hermes/'

    @abstractmethod
    def spawn_all_nodes(self):
        pass

    def cleanup(self):
        dirs = " ".join([os.path.join(d, '*') for d in self.devices.values()])
        Rm(dirs, LocalExecInfo(hostfile=self.spawn_all_nodes().hostfile))

    def find_tests(self):
        # Filter the list to include only attributes that start with "test"
        test_attributes = [attr for attr in dir(self) if
                           attr.startswith("test")]

        # Get a reference to each test method
        for attr in test_attributes:
            if callable(getattr(self, attr)):
                self.tests_[attr] = getattr(self, attr)

    def call(self, test_name):
        self.set_paths()
        if self.disable_testing:
            return
        test_name = test_name.strip()
        if test_name in self.tests_:
            print(f"Running test: {test_name}")
            exit_code = self.tests_[test_name]()
        else:
            print(f"{test_name} was not found. Available tests: ")
            for i, test in enumerate(self.tests_):
                print(f"{i}: {test}")
            exit_code = 1
            exit(1)
        self.cleanup()
        exit(exit_code)

    """======================================================================"""
    """ General Test Helper Functions """
    """======================================================================"""

    def start_daemon(self, spawn_info):
        """
        Helper function. Start the Hermes daemon

        :param env: Hermes environment variables
        :return: None
        """
        print("Killing daemon")
        Kill("hermes_daemon",
             LocalExecInfo(
                 hostfile=spawn_info.hostfile,
                 collect_output=False))

        print("Start daemon")
        self.daemon = Exec(f"{self.CMAKE_BINARY_DIR}/bin/hermes_daemon",
                           LocalExecInfo(
                               hostfile=spawn_info.hostfile,
                               env=spawn_info.basic_env,
                               exec_async=True))
        time.sleep(5)
        print("Launched")

    def stop_daemon(self, spawn_info):
        """
        Helper function. Stop the Hermes daemon.

        :param env: Hermes environment variables
        :return: None
        """
        print("Stop daemon")
        Exec(f"{self.CMAKE_BINARY_DIR}/bin/finalize_hermes",
             LocalExecInfo(
                 env=spawn_info.basic_env))
        self.daemon.wait()
        print("Stopped daemon")
