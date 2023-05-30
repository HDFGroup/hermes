from jarvis_util.shell.local_exec import LocalExec, LocalExecInfo

spawn_info = LocalExecInfo(hide_output=True)
LocalExec("echo hello", spawn_info)
