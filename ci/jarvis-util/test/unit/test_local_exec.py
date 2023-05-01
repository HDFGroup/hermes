import pathlib
import os
from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo
from jarvis_util.shell.exec import Exec


ret = LocalExec("echo hello", LocalExecInfo())
assert(str(ret.stdout).strip() != "hello")

ret = LocalExec("echo hello", LocalExecInfo())
ret = LocalExec("echo hello", LocalExecInfo(hide_output=True))
ret = LocalExec("echo hello", LocalExecInfo(pipe_stdout='/tmp/test.log',
                                            hide_output=True,
                                            collect_output=True))
assert(str(ret.stdout).strip() == "hello")

# node = Exec('gcc -print-file-name=libasan.so',
#             LocalExecInfo(collect_output=True, hide_output=True))
# assert(node.stdout == '/usr/lib/gcc/x86_64-linux-gnu/9/libasan.so')


HERE = str(pathlib.Path(__file__).parent.resolve())
PRINT10s = os.path.join(HERE, 'print10s.py')
ret = LocalExec(f"python3 {PRINT10s}",
                LocalExecInfo(collect_output=True,
                              pipe_stderr='/tmp/stderr.txt',
                              pipe_stdout='/tmp/stdout.txt'))

