"""
This file contains methods to automate large import __init__.py files
"""

import pathlib
import os


def _import_recurse(root_path, root, stmts):
    """
    Identify the set of files in the current "root" directory

    :param root_path: The path to the root of the python package
    :param root: The current subdirectory of the python package
    :param stmts: The current set of import statements
    :return:
    """
    for file in os.listdir(root):
        file = os.path.join(root, file)
        if os.path.isfile(file):
            file = os.path.relpath(file, root_path)
            ext = file.split('.')
            if ext[-1] == 'py':
                toks = ext[0].split('/')
                if toks[-1] == '__init__':
                    continue
                import_stmt = '.'.join(toks)
                stmts.append(f'from {import_stmt} import *')
        elif os.path.isdir(file):
            _import_recurse(root_path, file, stmts)
    return stmts


def import_all(root_path, root):
    """
    Create all import statement to do: from root import *.

    :param root_path: The root of the python repo
    :param root: The current directory we are in within the repo
    :return:
    """
    stmts = []
    _import_recurse(root_path, root, stmts)
    return '\"\"\"Import all modules\"\"\"\n' + '\n'.join(stmts) + '\n'


def build_global_import_file(root_path, pkg_name):
    """
    Build a file to be able to do: from pkg_name import *

    :param root_path: The path to the python package's root directory
    :param pkg_name: The name of the python package
    :return:
    """
    path = os.path.join(root_path, pkg_name)
    imports = import_all(root_path, path)
    with open(os.path.join(path, '__init__.py'), 'w',
              encoding='utf-8') as fp:
        fp.write(imports)


def build_global_import_from_bin(pkg_name):
    """
    Build a file to be able to do: from pkg_name import *
    This function is assumed to be called in the "bin" directory
    of the main python repo

    :param pkg_name: The name of the python package being built
    :return:
    """
    root_path = str(pathlib.Path(__file__).parent.parent.parent.resolve())
    build_global_import_file(root_path, pkg_name)
