"""
This module contains various wrappers over typical filesystem commands seen
in shell scripts. This includes operations such as creating directories,
changing file permissions, etc.
"""
from .exec import Exec


class Mkdir(Exec):
    """
    Create directories + subdirectories.
    """

    def __init__(self, paths, exec_info=None):
        """
        Create directories + subdirectories. Does not fail if the dirs
        already exist.

        :param paths: A list of paths or a single path string.
        :param exec_info: Info needed to execute the mkdir command
        """

        if isinstance(paths, str):
            paths = [paths]
        path = ' '.join(paths)
        super().__init__(f'mkdir -p {path}', exec_info)


class Rm(Exec):
    """
    Remove a file and its subdirectories
    """

    def __init__(self, paths, exec_info=None):
        """
        Execute file or directory remove.

        :param paths: Either a list of paths or a single path string
        :param exec_info: Information needed to execute rm
        """

        if isinstance(paths, str):
            paths = [paths]
        path = ' '.join(paths)
        super().__init__(f'rm -rf {path}', exec_info)


class Chmod(Exec):
    """
    Change the mode of a file
    """

    def __init__(self, path=None, mode=None, modes=None, exec_info=None):
        """
        Change the mode of a file

        :param path: path to file to mode change
        :param mode: the mode to change to
        :param modes: A list of tuples [(Path, Mode)]
        :param exec_info: How to execute commands
        """
        cmds = []
        if path is not None and mode is not None:
            cmds.append(f'chmod {mode} {path}')
        if modes is not None:
            cmds += [f'chmod {mode[1]} {mode[0]}' for mode in modes]
        if len(cmds) == 0:
            raise Exception('Must set either path+mode or modes')
        super().__init__(cmds, exec_info)
