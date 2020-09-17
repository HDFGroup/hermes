
void PlaceInHierarchy(SharedMemoryContext *context, RpcContext *rpc) {
  // Read data from PFS into blob
  // Call Put on that blob
}

void MoveToTarget(SharedMemoryContext *context, RpcContext *rpc,
                  TargetID dest) {
  // Move blob from current location to dest
}
