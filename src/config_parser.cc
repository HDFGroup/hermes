#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <string>

#include <glog/logging.h>

#include "hermes_types.h"
#include "memory_management.h"

// Steps to add a new configuration variable:
// 1. Add an entry to the ConfigVariable enum
// 2. Add the variable name to kConfigVariableStrings (currently the strings in
// this array must be in the same order as the corresponding entries in the
// ConfigVariable enum).
// 3. If a Parse<type> function does not exist for the variable type, implement
// it.
// 4. Add a case to ParseTokens for the new variable.

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

  Count
};

enum ConfigVariable {
  ConfigVariable_Unkown,
  ConfigVariable_NumTiers,
  ConfigVariable_Capacities,
  ConfigVariable_BlockSizes,
  ConfigVariable_NumSlabs,
  ConfigVariable_SlabUnitSizes,
  ConfigVariable_DesiredSlabPercentages,
  ConfigVariable_BandwidthsMbps,
  ConfigVariable_LatenciesUs,
  ConfigVariable_BufferPoolArenaPercentage,
  ConfigVariable_MetadataArenaPercentage,
  ConfigVariable_TransferWindowArenaPercentage,
  ConfigVariable_TransientArenaPercentage,
  ConfigVariable_MountPoints,
  ConfigVariable_RpcServerBaseName,
  ConfigVariable_BufferPoolShmemName,
  ConfigVariable_RpcProtocol,
  ConfigVariable_RpcPort,
  ConfigVariable_RpcHostNumberRange,

  ConfigVariable_Count
};

// TODO(chogan): Make this work independent of declaration order
static const char *kConfigVariableStrings[ConfigVariable_Count] = {
  "unknown",
  "num_tiers",
  "capacities_mb",
  "block_sizes_kb",
  "num_slabs",
  "slab_unit_sizes",
  "desired_slab_percentages",
  "bandwidths_mbps",
  "latencies_us",
  "buffer_pool_arena_percentage",
  "metadata_arena_percentage",
  "transfer_window_arena_percentage",
  "transient_arena_percentage",
  "mount_points",
  "rpc_server_base_name",
  "buffer_pool_shmem_name",
  "rpc_protocol",
  "rpc_port",
  "rpc_host_number_range",
};

struct Token {
  Token *next;
  char *data;
  u32 size;
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

EntireFile ReadEntireFile(Arena *arena, const char *path) {
  EntireFile result = {};
  FILE *fstream = fopen(path, "r");

  if (fstream) {
    int fseek_result = fseek(fstream, 0, SEEK_END);

    if (fseek_result == 0) {
      long file_size = ftell(fstream);

      if (file_size > 0) {

        if ((u32)file_size <= arena->capacity) {
          fseek(fstream, 0, SEEK_SET);
          result.data = PushArray<u8>(arena, file_size);
          [[maybe_unused]] int items_read = fread(result.data, file_size, 1, fstream);
          assert(items_read == 1);
          result.size = file_size;
        } else {
          LOG(FATAL) << "Arena capacity (" << arena->capacity
                     << ") too small to read file of size (" << file_size
                     << ")" << std::endl;
        }

      } else {
        // TODO(chogan): @errorhandling
        assert(!"ftell failed");
      }
    } else {
      // TODO(chogan): @errorhandling
      assert(!"fseek failed");
    }

    if (fclose(fstream) != 0) {
      // TODO(chogan): @errorhandling
    }

  } else {
    // TODO(chogan): @errorhandling
    assert(!"fopen failed");
  }

  return result;
}

void AddTokenToList(Arena *arena, TokenList *list, TokenType type) {
  Token *tok = PushClearedStruct<Token>(arena);
  tok->type = type;
  list->head->next = tok;
  list->head = tok;
}

inline bool BeginsComment(char c) {
  bool result = c == '#';

  return result;
}

inline bool EndOfComment(char c) {
  bool result = (c == '\n') || ( c == '\r' );

  return result;
}

inline bool IsWhitespace(char c) {
  bool result = c == ' ' || c == '\t' || c == '\n' || c == '\r';

  return result;
}

inline bool BeginsIdentifier(char c) {
  bool result = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c == '_');

  return result;
}

inline bool EndOfIdentifier(char c) {
  bool result = IsWhitespace(c) || c == '=';

  return result;
}

inline bool BeginsNumber(char c) {
  bool result = (c >= '0' && c <= '9') || (c == '.');

  return result;
}

inline bool EndOfNumber(char c) {
  bool result = IsWhitespace(c) || (c == ',') || (c == ';') || (c == '}');

  return result;
}

TokenList Tokenize(Arena *arena, EntireFile entire_file) {
  TokenList result = {};
  Token dummy = {};
  result.head = &dummy;

  char *at = (char *)entire_file.data;
  char *end = at + entire_file.size;

  while (at < end) {
    if (IsWhitespace(*at)) {
      ++at;
      continue;
    }

    if (BeginsComment(*at)) {
      while (at < end && !EndOfComment(*at)) {
        ++at;
      }
      continue;
    }

    if (BeginsIdentifier(*at)) {
      AddTokenToList(arena, &result, TokenType::Identifier);
      result.head->data = at;

      while (at && !EndOfIdentifier(*at)) {
        result.head->size++;
        at++;
      }
    } else if (BeginsNumber(*at)) {
      AddTokenToList(arena, &result, TokenType::Number);
      result.head->data = at;

      while (at && !EndOfNumber(*at)) {
        result.head->size++;
        at++;
      }
    } else {
      switch (*at) {
        case ';': {
          AddTokenToList(arena, &result, TokenType::Semicolon);
          break;
        }
        case '=': {
          AddTokenToList(arena, &result, TokenType::Equal);
          break;
        }
        case ',': {
          AddTokenToList(arena, &result, TokenType::Comma);
          break;
        }
        case '{': {
          AddTokenToList(arena, &result, TokenType::OpenCurlyBrace);
          break;
        }
        case '}': {
          AddTokenToList(arena, &result, TokenType::CloseCurlyBrace);
          break;
        }
        case '"': {
          AddTokenToList(arena, &result, TokenType::String);
          at++;
          result.head->data = at;

          while (at && *at != '"') {
            result.head->size++;
            at++;
          }
          break;
        }
        default: {
          assert(!"Unexpected token encountered\n");
          break;
        }
      }
      at++;
    }
    result.count++;
  }

  result.head = dummy.next;

  return result;
}

inline bool IsIdentifier(Token *tok) {
  bool result = tok->type == TokenType::Identifier;

  return result;
}

inline bool IsNumber(Token *tok) {
  bool result = tok->type == TokenType::Number;

  return result;
}

inline bool IsString(Token *tok) {
  bool result = tok->type == TokenType::String;

  return result;
}

inline bool IsOpenCurlyBrace(Token *tok) {
  bool result = tok->type == TokenType::OpenCurlyBrace;

  return result;
}

inline bool IsCloseCurlyBrace(Token *tok) {
  bool result = tok->type == TokenType::CloseCurlyBrace;

  return result;
}

inline bool IsComma(Token *tok) {
  bool result = tok->type == TokenType::Comma;

  return result;
}

inline bool IsEqual(Token *tok) {
  bool result = tok->type == TokenType::Equal;

  return result;
}

inline bool IsSemicolon(Token *tok) {
  bool result = tok->type == TokenType::Semicolon;

  return result;
}

void PrintExpectedAndFail(const std::string &expected) {
  LOG(FATAL) << "Configuration parser expected: " << expected << std::endl;
}

ConfigVariable GetConfigVariable(Token *tok) {
  bool found = false;
  ConfigVariable result = ConfigVariable_Unkown;

  // TODO(chogan): Hash table would be faster, but this is already instantaneous
  for (int i = 1; i < ConfigVariable_Count; ++i) {
    if (strncmp(tok->data, kConfigVariableStrings[i], tok->size) == 0) {
      result = (ConfigVariable)i;
      found = true;
    }
  }

  if (!found) {
    LOG(WARNING) << "Ignoring unrecognized configuration variable: "
                 << std::string(tok->data, tok->size) << std::endl;
  }

  return result;
}

size_t ParseSizet(Token **tok) {
  size_t result = 0;
  if (*tok && IsNumber(*tok)) {
    if (sscanf((*tok)->data, "%zu", &result) == 1) {
      *tok = (*tok)->next;
    } else {
      LOG(FATAL) << "Could not interpret token data '"
                 << std::string((*tok)->data, (*tok)->size) << "' as a size_t"
                 << std::endl;
    }
  } else {
    PrintExpectedAndFail("a number");
  }

  return result;
}

Token *ParseSizetList(Token *tok, size_t *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseSizet(&tok);
      if (i != n - 1) {
        if (IsComma(tok)) {
          tok = tok->next;
        } else {
          PrintExpectedAndFail(",");
        }
      }
    }

    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}");
    }
  } else {
    PrintExpectedAndFail("{");
  }

  return tok;
}

int ParseInt(Token **tok) {
  long result = 0;
  if (*tok && IsNumber(*tok)) {
    errno = 0;
    result = strtol((*tok)->data, NULL, 0);
    if (errno == ERANGE || (result == 0 && errno != 0) || result >= INT_MAX) {
      PrintExpectedAndFail("an integer between 0 and INT_MAX");
    }
    *tok = (*tok)->next;
  } else {
    PrintExpectedAndFail("a number");
  }

  return (int)result;
}

Token *ParseIntList(Token *tok, int *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseInt(&tok);
      if (i != n - 1) {
        if (IsComma(tok)) {
          tok = tok->next;
        } else {
          PrintExpectedAndFail(",");
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}");
    }
  } else {
    PrintExpectedAndFail("{");
  }

  return tok;
}

Token *ParseIntListList(Token *tok, int out[][hermes::kMaxBufferPoolSlabs],
                        int n, int *m) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      tok = ParseIntList(tok, out[i], m[i]);
      if (i != n - 1) {
        assert(IsComma(tok));
        tok = tok->next;
      } else {
        // Optional final comma
        if (IsComma(tok)) {
          tok = tok->next;
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}");
    }
  } else {
    PrintExpectedAndFail("{");
  }

  return tok;
}

f32 ParseFloat(Token **tok) {
  double result = 0;
  if (*tok && IsNumber(*tok)) {
    result = strtod((*tok)->data, NULL);
    if (result <= 0 || errno == ERANGE || result > FLT_MAX) {
      PrintExpectedAndFail("a floating point number between 1 and FLT_MAX");
    }
    *tok = (*tok)->next;
  } else {
    PrintExpectedAndFail("a number");
  }

  return (f32)result;
}

Token *ParseFloatList(Token *tok, f32 *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseFloat(&tok);
      if (i != n - 1) {
        if (IsComma(tok)) {
          tok = tok->next;
        } else {
          PrintExpectedAndFail(",");
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}");
    }
  } else {
    PrintExpectedAndFail("{");
  }

  return tok;
}

Token *ParseFloatListList(Token *tok, f32 out[][hermes::kMaxBufferPoolSlabs],
                          int n, int *m) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      tok = ParseFloatList(tok, out[i], m[i]);
      if (i != n - 1) {
        if (IsComma(tok)) {
          tok = tok->next;
        } else {
          PrintExpectedAndFail(",");
        }
      } else {
        // Optional final comma
        if (IsComma(tok)) {
          tok = tok->next;
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}");
    }
  } else {
    PrintExpectedAndFail("{");
  }

  return tok;
}

std::string ParseString(Token **tok) {
  std::string result;
  if (*tok && IsString(*tok)) {
    result = std::string((*tok)->data, (*tok)->size);
    *tok = (*tok)->next;
  } else {
    PrintExpectedAndFail("a string");
  }

  return result;
}

Token *ParseStringList(Token *tok, std::string *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseString(&tok);
      if (i != n - 1) {
        if (IsComma(tok)) {
          tok = tok->next;
        } else {
          PrintExpectedAndFail(",");
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}");
    }
  } else {
    PrintExpectedAndFail("{");
  }

  return tok;
}

Token *ParseCharArrayString(Token *tok, char *arr) {
  if (tok && IsString(tok)) {
    strncpy(arr, tok->data, tok->size);
    arr[tok->size] = '\0';
    tok = tok->next;
  } else {
    PrintExpectedAndFail("a string");
  }

  return tok;
}

void RequireNumTiers(Config *config) {
  if (config->num_tiers == 0) {
    LOG(FATAL) << "The configuration variable 'num_tiers' must be defined first"
               << std::endl;
  }
}

void RequireNumSlabs(Config *config) {
  if (config->num_slabs == 0) {
    LOG(FATAL) << "The configuration variable 'num_slabs' must be defined first"
               << std::endl;
  }
}

Token *BeginStatement(Token *tok) {
  if (tok && IsIdentifier(tok)) {
    tok = tok->next;
    if (tok && IsEqual(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("=");
    }
  } else {
    PrintExpectedAndFail("an identifier");
  }

  return tok;
}

Token *EndStatement(Token *tok) {
  if (tok && IsSemicolon(tok)) {
    tok = tok->next;
  } else {
    PrintExpectedAndFail(";");
  }

  return tok;
}

void ParseTokens(TokenList *tokens, Config *config) {

  Token *tok = tokens->head;
  while (tok) {
    ConfigVariable var = GetConfigVariable(tok);

    if (var == ConfigVariable_Unkown) {
      tok = tok->next;
      while (tok && !IsIdentifier(tok)) {
        tok = tok->next;
      }
      continue;
    }

    tok = BeginStatement(tok);

    switch (var) {
      case ConfigVariable_NumTiers: {
        int val = ParseInt(&tok);
        config->num_tiers = val;
        break;
      }
      case ConfigVariable_Capacities: {
        RequireNumTiers(config);
        tok = ParseSizetList(tok, config->capacities, config->num_tiers);
        break;
      }
      case ConfigVariable_BlockSizes: {
        RequireNumTiers(config);
        tok = ParseIntList(tok, config->block_sizes, config->num_tiers);
        break;
      }
      case ConfigVariable_NumSlabs: {
        RequireNumTiers(config);
        tok = ParseIntList(tok, config->num_slabs, config->num_tiers);
        break;
      }
      case ConfigVariable_SlabUnitSizes: {
        RequireNumTiers(config);
        RequireNumSlabs(config);
        tok = ParseIntListList(tok, config->slab_unit_sizes, config->num_tiers,
                               config->num_slabs);
        break;
      }
      case ConfigVariable_DesiredSlabPercentages: {
        RequireNumTiers(config);
        RequireNumSlabs(config);
        tok = ParseFloatListList(tok, config->desired_slab_percentages,
                                 config->num_tiers, config->num_slabs);
        break;
      }
      case ConfigVariable_BandwidthsMbps: {
        RequireNumTiers(config);
        tok = ParseFloatList(tok, config->bandwidths, config->num_tiers);
        break;
      }
      case ConfigVariable_LatenciesUs: {
        RequireNumTiers(config);
        tok = ParseFloatList(tok, config->latencies, config->num_tiers);
        break;
      }
      case ConfigVariable_BufferPoolArenaPercentage: {
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_BufferPool] = val;
        break;
      }
      case ConfigVariable_MetadataArenaPercentage: {
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_MetaData] = val;
        break;
      }
      case ConfigVariable_TransferWindowArenaPercentage: {
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_TransferWindow] = val;
        break;
      }
      case ConfigVariable_TransientArenaPercentage: {
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_Transient] = val;
        break;
      }
      case ConfigVariable_MountPoints: {
        RequireNumTiers(config);
        tok = ParseStringList(tok, config->mount_points, config->num_tiers);
        break;
      }
      case ConfigVariable_RpcServerBaseName: {
        config->rpc_server_base_name = ParseString(&tok);
        break;
      }
      case ConfigVariable_BufferPoolShmemName: {
        tok = ParseCharArrayString(tok, config->buffer_pool_shmem_name);
        break;
      }
      case ConfigVariable_RpcProtocol: {
        config->rpc_protocol = ParseString(&tok);
        break;
      }
      case ConfigVariable_RpcPort: {
        config->rpc_port = ParseInt(&tok);
        break;
      }
      case ConfigVariable_RpcHostNumberRange: {
        tok = ParseIntList(tok, config->rpc_host_number_range, 2);
        break;
      }
      default: {
        assert(!"Invalid code path\n");
        break;
      }
    }
    tok = EndStatement(tok);
  }
}

void ParseConfig(Arena *arena, const char *path, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  EntireFile config_file = ReadEntireFile(scratch, path);
  TokenList tokens = Tokenize(scratch, config_file);
  ParseTokens(&tokens, config);
}

}  // namespace hermes
