"""
This module contains methods to create strings which follow a particular
naming convention.
"""

import re


def to_camel_case(string):
    """
    Convert a string in snake case to camel case

    :param string:
    :return:
    """
    if string is None:
        return
    words = re.sub(r'(_|-)+', ' ', string).split()
    words = [word.capitalize() for word in words]
    return ''.join(words)


def to_snake_case(string):
    """
    Convert a string in CamelCase to snake case
    :param string:
    :return:
    """
    if string is None:
        return
    words = re.split('([A-Z][a-z0-9_]*)', string)
    words = [word for word in words if len(word)]
    string = '_'.join(words)
    return string.lower()
