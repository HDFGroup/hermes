"""
This file contains helper methods to load a class dynamically from a file
"""

import sys

# NOTE(llogan): To get the path of the directory this file is in, use
# str(pathlib.Path(__file__).parent.resolve())


def load_class(import_str, path, class_name):
    """
    Loads a class from a python file.

    :param import_str: A python import string. E.g., for "myrepo.dir1.pkg"
    :param path: The absolute path to the directory which contains the
    beginning of the import statement. Let's say you have a git repo located
    at "/home/hello/myrepo". The git repo has a subdirectory called "myrepo",
    so "/home/hello/myrepo/myrepo". In this case, path would be
    "/home/hello/myrepo". The import string "myrepo.dir1.pkg" will find
    the "myrepo" part of the import string at "/home/hello/myrepo/myrepo".
    :param class_name: The name of the class in the file
    :return:
    """
    sys.path.insert(0, path)
    module = __import__(import_str, fromlist=[class_name])
    sys.path.pop(0)
    return getattr(module, class_name)
