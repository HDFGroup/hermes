"""
This module provides methods to convert a semantic size string to an integer.
"""


class SizeConv:
    """
    A class which provides methods to convert a semantic size string to an int.
    """

    @staticmethod
    def to_int(text):
        text = text.lower()
        if 'k' in text:
            return SizeConv.kb(text)
        if 'm' in text:
            return SizeConv.mb(text)
        if 'g' in text:
            return SizeConv.gb(text)
        if 't' in text:
            return SizeConv.tb(text)
        if 'p' in text:
            return SizeConv.pb(text)
        return int(text)

    @staticmethod
    def kb(num):
        return int(float(num.split('k')[0]) * (1 << 10))

    @staticmethod
    def mb(num):
        return int(float(num.split('m')[0]) * (1 << 20))

    @staticmethod
    def gb(num):
        return int(float(num.split('g')[0]) * (1 << 30))

    @staticmethod
    def tb(num):
        return int(float(num.split('t')[0]) * (1 << 40))

    @staticmethod
    def pb(num):
        return int(float(num.split('p')[0]) * (1 << 50))
