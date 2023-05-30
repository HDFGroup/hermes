"""
This module contains functions for expanding environment variables for
dictionaries
"""

import os


def expand_env(data):
    """
    Expand environment variables for dictionaries

    :param data: A dict where strings may contain environment variables to
    expand
    :return:
    """
    if isinstance(data, str):
        return os.path.expandvars(data)
    if isinstance(data, dict):
        for key, val in data.items():
            data[key] = expand_env(val)
    if isinstance(data, (list, tuple)):
        for i, val in enumerate(data):
            data[i] = expand_env(val)
    return data
