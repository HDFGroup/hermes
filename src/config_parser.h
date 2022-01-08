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

#ifndef HERMES_CONFIG_PARSER_H_
#define HERMES_CONFIG_PARSER_H_

namespace hermes {

enum class TokenType {
  Identifier,
  Number,
  String,
  OpenCurlyBrace,
  CloseCurlyBrace,
  Comma,
  Equal,
  Semicolon,
  Hyphen,

  Count
};

enum ConfigVariable {
  ConfigVariable_Unkown,
  ConfigVariable_NumDevices,
  ConfigVariable_NumTargets,
  ConfigVariable_CapacitiesB,
  ConfigVariable_CapacitiesKb,
  ConfigVariable_CapacitiesMb,
  ConfigVariable_CapacitiesGb,
  ConfigVariable_BlockSizesB,
  ConfigVariable_BlockSizesKb,
  ConfigVariable_BlockSizesMb,
  ConfigVariable_BlockSizesGb,
  ConfigVariable_NumSlabs,
  ConfigVariable_SlabUnitSizes,
  ConfigVariable_DesiredSlabPercentages,
  ConfigVariable_BandwidthsMbps,
  ConfigVariable_LatenciesUs,
  ConfigVariable_BufferPoolArenaPercentage,
  ConfigVariable_MetadataArenaPercentage,
  ConfigVariable_TransientArenaPercentage,
  ConfigVariable_MountPoints,
  ConfigVariable_SwapMount,
  ConfigVariable_NumBufferOrganizerRetries,
  ConfigVariable_MaxBucketsPerNode,
  ConfigVariable_MaxVBucketsPerNode,
  ConfigVariable_SystemViewStateUpdateInterval,
  ConfigVariable_RpcServerBaseName,
  ConfigVariable_RpcServerSuffix,
  ConfigVariable_BufferPoolShmemName,
  ConfigVariable_RpcProtocol,
  ConfigVariable_RpcDomain,
  ConfigVariable_RpcPort,
  ConfigVariable_BufferOrganizerPort,
  ConfigVariable_RpcHostNumberRange,
  ConfigVariable_RpcNumThreads,
  ConfigVariable_PlacementPolicy,
  ConfigVariable_IsSharedDevice,
  ConfigVariable_BoNumThreads,
  ConfigVariable_RRSplit,

  ConfigVariable_Count
};

struct Token {
  Token *next;
  char *data;
  u32 size;
  u32 line;
  TokenType type;
};

struct TokenList {
  Token *head;
  int count;
};

struct EntireFile {
  u8 *data;
  u64 size;
};

TokenList Tokenize(Arena *arena, EntireFile entire_file);
void ParseTokens(TokenList *tokens, Config *config);

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
