from code_generators.decorator.parse_cpp import ParseDecoratedCppApis
from code_generators.decorator.decorator import NullDecorator
from code_generators.rpc.generator import RpcGenerator
from code_generators.util.paths import HERMES_ROOT


def create_rpc_generator():
    gen = RpcGenerator()
    rpc_defs_path = f"{HERMES_ROOT}/code_generators/unit/rpcs.cc"
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
    gen.add("Localf101", "6")

    gen.set_class("nstest::Hi", "hi")
    gen.add("Localf100", "7")

    return gen

files = [
    f"{HERMES_ROOT}/code_generators/unit/api/apis.h"
]

decs = [
    create_rpc_generator(),
    NullDecorator("WRAP")
]

gen = ParseDecoratedCppApis(files, decs)
gen.parse()