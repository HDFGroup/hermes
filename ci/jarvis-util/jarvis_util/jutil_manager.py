"""
This file contains properties which are globally accessible to all
jarvis-util modules. This can be used to configure various aspects
of jarvis, such as output.
"""


class JutilManager:
    """
    A singleton which stores various properties that can be queried by
    internally by jutil modules. This includes properties such output
    management.
    """

    instance_ = None

    @staticmethod
    def get_instance():
        if JutilManager.instance_ is None:
            JutilManager.instance_ = JutilManager()
        return JutilManager.instance_

    def __init__(self):
        self.collect_output = False
        self.hide_output = False
        self.debug_mpi_exec = False
        self.debug_local_exec = False

