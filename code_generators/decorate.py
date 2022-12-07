"""
USAGE:
    cd /path/to/code_generators
    python3 decorate.py
"""

from code_generators.decorator.parse_cpp import ParseDecoratedCppApis
from code_generators.rpc.generator import RpcGenerator
from code_generators.util.paths import HERMES_ROOT

class HermesDecorator:
    def __init__(self):
        files = [
            f"{HERMES_ROOT}/src/metadata_manager.h",
            f"{HERMES_ROOT}/src/rpc_thallium_defs.cc"
        ]
        decs = [
            self.create_rpc_generator()
        ]
        gen = ParseDecoratedCppApis(files, decs, modify=True)
        gen.parse()

    @staticmethod
    def create_rpc_generator():
        rpc_defs_path = f"{HERMES_ROOT}/src/rpc_thallium_defs.cc"
        return RpcGenerator(rpc_defs_path)

HermesDecorator()