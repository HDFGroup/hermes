from code_generators.decorator.parse_cpp import ParseDecoratedCppApis
from code_generators.decorator.decorator import NullDecorator
from code_generators.rpc.generator import RpcGenerator
from code_generators.util.paths import HERMES_ROOT


def create_rpc_generator():
    gen = RpcGenerator()
    rpc_defs_path = f"{HERMES_ROOT}/code_generators/unit/api/rpcs.cc"
    gen.set_rpc_lambda_file(rpc_defs_path)

    gen.set_file(f"{HERMES_ROOT}/code_generators/unit/api/apis.h")
    gen.set_class(None, None)
    gen.add("Localf2", "0")
    gen.add("Localf3", "1")
    gen.add("Localf4", "2")
    gen.add("Localf5", "3")
    gen.add("Localf6", "4")

    gen.set_class("nstest", None)
    gen.add("Localf99", "5")
    gen.add("Localf102", "6")

    gen.set_class("nstest::Hi", "hi")
    gen.add("Localf100", "7")

    gen.set_class("nstest::BigHi", "hi2")
    gen.add("Localf101", "8")

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
