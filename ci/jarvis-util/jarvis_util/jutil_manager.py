import threading
import sys
import time


class JutilManager:
    instance_ = None

    @staticmethod
    def get_instance():
        if JutilManager.instance_ is None:
            JutilManager.instance_ = JutilManager()
        return JutilManager.instance_

    def __init__(self):
        self.collect_output = False
        self.hide_output = False
        self.print_thread = None
        self.continue_ = True
        self.print_tasks = []

    def monitor_print(self, local_exec):
        if len(self.print_tasks) == 0:
            self.print_thread = threading.Thread(target=self.print_worker)
            self.continue_ = True
            self.print_thread.start()
        self.print_tasks.append(local_exec)

    def unmonitor_print(self, local_exec):
        self.print_tasks.remove(local_exec)
        self.print_to_outputs(local_exec)
        local_exec.stdout = local_exec.stdout.getvalue()
        local_exec.stderr = local_exec.stderr.getvalue()
        if len(self.print_tasks) == 0:
            self.continue_ = False
            self.print_thread.join()

    def print_worker(self):
        while self.continue_:
            for local_exec in self.print_tasks:
                self.print_to_outputs(local_exec)
            # time.sleep(25 / 1000)

    def print_to_outputs(self, local_exec):
        self.print_to_output(local_exec, local_exec.proc.stdout, sys.stdout)
        self.print_to_output(local_exec, local_exec.proc.stderr, sys.stderr)

    def print_to_output(self, local_exec, out, sysout):
        if len(out.peek()) == 0:
            return
        text = out.read().decode('utf-8')
        if not local_exec.hide_output:
            sysout.write(text)
        if local_exec.pipe_stdout:
            local_exec.stdout.write(text)
        if local_exec.file_output is not None:
            local_exec.file_output.write(text)
