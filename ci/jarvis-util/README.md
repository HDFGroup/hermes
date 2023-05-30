# Jarvis UTIL

Jarvis-util is a library which contains various utilities to aid with
creating shell scripts within Python. This library contains wrappers
for executing shell commands locally, SSH, SCP, MPI, argument parsing, 
and various other random utilities.

![Build](https://github.com/lukemartinlogan/jarvis-util/workflows/GitHub%20Actions/badge.svg)

[![Coverage Status](https://coveralls.io/repos/github/lukemartinlogan/jarvis-util/badge.svg?branch=master)](https://coveralls.io/github/lukemartinlogan/jarvis-util?branch=master)

## Installation

For now, we only consider manual installation
```bash
cd jarvis-util
python3 -m pip install -r requirements.txt
python3 -m pip install -e .
```

## Executing a program

The following code will execute a command on the local machine.
The output will NOT be collected into an in-memory buffer.
The output will be printed to the terminal as it occurs.

```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo 

node = Exec('echo hello', LocalExecInfo(collect_output=False))
```

Programs can also be executed asynchronously:
```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo 

node = Exec('echo hello', LocalExecInfo(collect_output=False,
                                        exec_async=True))
node.wait()
```

Various examples of outputs:
```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo 

# Will ONLY print to the terminal
node = Exec('echo hello', LocalExecInfo(collect_output=False))
# Will collect AND print to the terminal
node = Exec('echo hello', LocalExecInfo(collect_output=True))
# Will collect BUT NOT print to the terminal
node = Exec('echo hello', LocalExecInfo(collect_output=True,
                                        hide_output=True))
# Will collect, pipe to file, and print to terminal
node = Exec('echo hello', LocalExecInfo(collect_output=True,
                                        pipe_stdout='/tmp/stdout.txt',
                                        pipe_stderr='/tmp/stderr.txt'))
```

This is useful if you have a program which runs using a daemon mode.

## Executing an MPI program

The following code will execute the "hostname" command on the local
machine 24 times using MPI.

```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.mpi_exec import MpiExecInfo 

node = Exec('hostname', MpiExecInfo(hostfile=None,
                                    nprocs=24,
                                    ppn=None,
                                    collect_output=False))
```

## Executing an SSH program

The following code will execute the "hostname" command on all machines
in the hostfile

```python
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.pssh_exec import PsshExecInfo 

node = Exec('hostname', PsshExecInfo(hostfile="/tmp/hostfile.txt",
                                     collect_output=False))
```

## The contents of a hostfile

A hostfile can have the following syntax:
```
ares-comp-01
ares-comp-[02-04]
ares-comp-[05-09,11,12-14]-40g
```

These will be expanded internally by PSSH and MPI.

# Unit tests

We run our unit tests in a docker container, which is located underneath
the CI directory. This is because we need to test multi-node functionality,
without having multiple nodes. To setup unit testing, perform the following:

1. Install Docker
2. Setup our "ci" container
3. Run the unit tests

```
```

# Contributing

We use the Google Python Style guide (pylintrc).