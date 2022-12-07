from code_generators.decorator.parse_docstring import ParseDocstring


class RpcDocstring(ParseDocstring):
    def __init__(self, api):
        super().__init__(api)
        self.target_node = None
        self.class_instance = None

    def parse(self):
        self.target_node = self.get_param("RPC_HASH")
        self.class_instance = self.get_param("RPC_CLASS")
