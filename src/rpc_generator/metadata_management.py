
from rpc_generator.rpc_generator import RpcGenerator

gen = RpcGenerator()
gen.SetFile("metadata_management.h")
gen.Add("BucketID LocalGetOrCreateBucketId(const std::string &name);")