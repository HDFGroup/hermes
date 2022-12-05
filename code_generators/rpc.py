
"""
Automatically generate the RPCs for a series of Local functions

USAGE:
    cd code_generators/bin
    python3 rpc.py [whether or not to modify path or make tmpfiles]
"""

import sys
from code_generators.rpc.generator import RpcGenerator
from code_generators.util.paths import HERMES_ROOT

rpc_defs_path = f"{HERMES_ROOT}/src/rpc_thallium_defs.cc"
if len(sys.argv) < 2:
    gen = RpcGenerator(rpc_defs_path, False)
else:
    gen = RpcGenerator(rpc_defs_path, sys.argv[1])

gen.set_file(f"{HERMES_ROOT}/src/metadata_manager.h", "MetadataManager", "mdm")
gen.add("LocalGetOrCreateBucket", "0")

gen.generate()