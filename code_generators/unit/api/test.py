from code_generators.decorator.parse_cpp import ParseDecoratedCppApis
from code_generators.decorator.decorator import NullDecorator
from code_generators.rpc.generator import RpcGenerator
from code_generators.util.paths import HERMES_ROOT


def create_rpc_generator():
    rpc_defs_path = f"{HERMES_ROOT}/code_generators/unit/api/rpcs.cc"
    gen = RpcGenerator(rpc_defs_path)
    return gen

files = [
    f"{HERMES_ROOT}/code_generators/unit/api/apis.h",
    f"{HERMES_ROOT}/code_generators/unit/api/rpcs.cc",
]

decs = [
    create_rpc_generator(),
    NullDecorator("WRAP")
]

gen = ParseDecoratedCppApis(files, decs, modify=False)
gen.parse()
print(gen.text_map[files[0]])
print(gen.text_map[files[1]])
