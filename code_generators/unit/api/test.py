from code_generators.decorator.parse_cpp import ParseDecoratedCppApis
from code_generators.decorator.decorator import NullDecorator
from code_generators.rpc.generator import RpcGenerator
from code_generators.util.paths import HERMES_ROOT


class ParseCppUnitTest:
    def __init__(self):
        self.files = [
            f"{HERMES_ROOT}/code_generators/unit/api/apis.h",
            f"{HERMES_ROOT}/code_generators/unit/api/rpcs.cc",
        ]
        self.decs = [
            self.create_rpc_generator(),
            NullDecorator("WRAP")
        ]
        self.gen = ParseDecoratedCppApis(self.files, self.decs, modify=True)
        self.gen.build_api_map()
        self.verify_namespaces()
        self.verify_apis()
        self.gen.parse()
        print(self.gen.text_map[self.files[0]])
        print(self.gen.text_map[self.files[1]])

    def create_rpc_generator(self):
        rpc_defs_path = f"{HERMES_ROOT}/code_generators/unit/api/rpcs.cc"
        gen = RpcGenerator(rpc_defs_path)
        return gen

    def verify_namespaces(self):
        namespaces = [
            None, "nstest", "nstest",
            "nstest::nstest2::Hi", "nstest::BigHi",
        ]

        for namespace in namespaces:
            if namespace not in self.gen.api_map[self.files[0]]:
                raise Exception(f"Could not find {namespace} in api_map")

    def verify_apis(self):
        ignore_apis = {
            None: ['Localf1']
        }
        parse_apis = {
            None: ['Localf2', 'Localf2', 'Localf4', 'Localf5', 'Localf6'],
            'nstest': ['Localf99', 'Localf102'],
            'nstest::nstest2::Hi': ['Localf100'],
            'nstest::BigHi': ['Localf101']
        }

        for namespace, api_list in parse_apis.items():
            for api in api_list:
                if api not in self.gen.api_map[self.files[0]][namespace]['apis']:
                    raise Exception(f"Could not find {api} in api_map")


ParseCppUnitTest()
