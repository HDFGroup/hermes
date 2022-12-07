from abc import ABC, abstractmethod


class ApiDecorator(ABC):
    def __init__(self, macro):
        """
        :param api_dec: the string representing the decorator macro to
        search for in the CPP files
        """

        self.macro = macro
        self.autogen_dec_start = f"{self.macro}_AUTOGEN_START"
        self.autogen_dec_end = f"{self.macro}_AUTOGEN_END"

    @abstractmethod
    def modify(self, api_map):
        """
        Generate RPCs in the files

        :param api_map: [file][namespace] -> {
            'apis': [api_name] -> API
            'start': start of autogen region
            'end': end of autogen region
        }
        """
        pass


class NullDecorator(ApiDecorator):
    def modify(self, api_map):
        pass
