from abc import ABC, abstractmethod


class ApiDecorator(ABC):
    def __init__(self, api_dec):
        self.api_dec = api_dec
        self.autogen_dec_start = f"{self.api_dec}_AUTOGEN_START"
        self.autogen_dec_end = f"{self.api_dec}_AUTOGEN_END"

    @abstractmethod
    def init_api(self, api):
        """
        Add additional information to the Api object when it's registered
        for processing
        """
        pass

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