import pathlib
import os
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo
from jarvis_util.shell.exec import Exec
from unittest import TestCase


class TestLocalExec(TestCase):
    def _setup_files(self):
        self.stdout = '/tmp/test_out.log'
        self.stderr = '/tmp/test_err.log'
        try:
            os.remove(self.stdout)
        except OSError:
            pass
        try:
            os.remove(self.stderr)
        except:
            pass

    def test_default(self):
        ret = Exec("echo hello")
        self.assertEqual(ret.exit_code, 0)
        self.assertEqual(len(ret.stdout['localhost']), 0)

    def test_pipe_stdout(self):
        self._setup_files()
        spawn_info = LocalExecInfo(pipe_stdout=self.stdout,
                                   pipe_stderr=self.stderr,
                                   collect_output=True)
        ret = Exec("echo hello", spawn_info)
        self.assertEqual(ret.stdout['localhost'].strip(), "hello")
        self.assertEqual(ret.stderr['localhost'].strip(), "")
        self.assertFile(self.stdout, "hello")
        self.assertFile(self.stderr, "")

    def test_hide_stdout(self):
        HERE = str(pathlib.Path(__file__).parent.resolve())
        PRINTNONE = os.path.join(HERE, 'printNone.py')
        spawn_info = LocalExecInfo(collect_output=True)
        ret = Exec(f"python3 {PRINTNONE}", spawn_info)
        self.assertEqual(ret.stdout['localhost'].strip(), "")
        self.assertEqual(ret.stderr['localhost'].strip(), "")

    def test_periodic_print(self):
        self._setup_files()
        HERE = str(pathlib.Path(__file__).parent.resolve())
        PRINT5s = os.path.join(HERE, 'print5s.py')
        ret = Exec(f"python3 {PRINT5s}",
                   LocalExecInfo(pipe_stdout=self.stdout,
                                 pipe_stderr=self.stderr))
        stdout_data = "\n".join([f"COUT: {i}" for i in range(5)])
        stderr_data = "\n".join([f"CERR: {i}" for i in range(5)])
        self.assertFile(self.stdout, stdout_data)
        self.assertFile(self.stderr, stderr_data)

    def assertFile(self, path, data, strip=True):
        self.assertTrue(os.path.exists(path))
        with open(path, 'r') as fp:
            if strip:
                data = data.strip()
                file_data = fp.read().strip()
            else:
                file_data = fp.read()
        self.assertEqual(data, file_data)
