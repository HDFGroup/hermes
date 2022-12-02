
"""
Automatically generate the RPCs for a series of Local functions

USAGE:
    cd /path/to/rpc_generator
    python3 generate.py
"""

from rpc_generator.rpc_generator import RpcGenerator

gen = RpcGenerator()

gen.set_file("../buffer_organizer.h", "BufferOrganizer", "borg")
gen.add("LocalGetBufferInfo", "buffer_id.bits.node_id")
gen.add("LocalEnqueueBoMove", "rpc_->node_id_")
gen.add("LocalOrganizeBlob", "HashString(internal_name.c_str())")
gen.add("LocalEnforceCapacityThresholds", "info.target_id.bits.node_id")
gen.add("LocalEnqueueFlushingTask", "rpc->node_id_")
gen.add("LocalIncrementFlushCount", "HashString(vbkt_name.c_str())")
gen.add("LocalDecrementFlushCount", "HashString(vbkt_name.c_str())")

# gen.set_file("metadata_management.h", "METADTA_MANAGER_RPC", "mdm")
# gen.add("LocalGetVBucketInfoById", "HashString(name)")

gen.generate()