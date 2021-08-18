/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_METADATA_STORAGE_H_
#define HERMES_METADATA_STORAGE_H_

namespace hermes {

/**
 *
 */
void PutToStorage(MetadataManager *mdm, const char *key, u64 val,
                  MapType map_type);

/**
 *
 */
void PutToStorage(MetadataManager *mdm, BlobID key, const BlobInfo &val);

/**
 *
 */
u64 GetFromStorage(MetadataManager *mdm, const char *key, MapType map_type);

/**
 *
 */
std::string ReverseGetFromStorage(MetadataManager *mdm, u64 id,
                                  MapType map_type);

/**
 *
 */
void DeleteFromStorage(MetadataManager *mdm, BlobID key, bool lock);

/**
 *
 */
void DeleteFromStorage(MetadataManager *mdm, const char *key,
                       MapType map_type);

/**
 *
 */
u32 HashStringForStorage(MetadataManager *mdm, RpcContext *rpc,
                         const char *str);

/**
 *
 */
void SeedHashForStorage(size_t seed);

/**
 *
 */
size_t GetStoredMapSize(MetadataManager *mdm, MapType map_type);

/**
 *
 */
std::vector<TargetID> LocalGetNodeTargets(SharedMemoryContext *context);

/**
 *
 */
std::vector<TargetID> LocalGetNeighborhoodTargets(SharedMemoryContext *context);

/**
 *
 */
std::vector<BlobID> LocalGetBlobIds(SharedMemoryContext *context,
                                    BucketID bucket_id);
/**
 *
 */
BlobInfo *GetBlobInfoPtr(MetadataManager *mdm, BlobID blob_id);

/**
 *
 */
void ReleaseBlobInfoPtr(MetadataManager *mdm);

}  // namespace hermes

#endif  // HERMES_METADATA_STORAGE_H_
