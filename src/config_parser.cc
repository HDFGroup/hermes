#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hermes_types.h"
#include "memory_arena.h"

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
  ConfigVariable_RpcServerName,

  ConfigVariable_Count
};

static const char *kConfigVariableStrings[ConfigVariable_Count] = {
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
  "rpc_server_name",
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

        if (file_size <= arena->capacity) {
          fseek(fstream, 0, SEEK_SET);
          result.data = PushArray<u8>(arena, file_size);
          int items_read = fread(result.data, file_size, 1, fstream);
          assert(items_read == 1);
          result.size = file_size;
        } else {
          // TODO(chogan): @logging
        }

      } else {
        // TODO(chogan): @errorhandling
        assert(!"ftell failed");
      }
    } else {
      // TODO(chogan): @errorhandling
      assert(!"fseek failed");
    }

    int fclose_result = fclose(fstream);
    // TODO(chogan): @errorhandling

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

ConfigVariable GetConfigVariable(Token *tok) {
  bool found = false;
  ConfigVariable result;

  // TODO(chogan): Hash table would be faster, but this is already instantaneous
  for (int i = 0; i < ConfigVariable_Count; ++i) {
    if (strncmp(tok->data, kConfigVariableStrings[i], tok->size) == 0) {
      result = (ConfigVariable)i;
      found = true;
    }
  }

  if (!found) {
    // TODO(chogan): @logging
    // TODO(chogan): @errorhandling
    exit(1);
  }

  return result;
}

size_t ParseSizet(Token **tok) {
  size_t result = 0;
  if (*tok && IsNumber(*tok)) {
    if (sscanf((*tok)->data, "%zu", &result) == 1) {
      *tok = (*tok)->next;
    } else {
      // TODO(chogan): @errorhandling
    }
  } else {
    // TODO(chogan): @errorhandling
  }

  return result;
}

Token *ParseSizetList(Token *tok, size_t *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseSizet(&tok);
      if (i != n - 1) {
        assert(IsComma(tok));
        tok = tok->next;
      }
    }
    assert(IsCloseCurlyBrace(tok));
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return tok;
}

int ParseInt(Token **tok) {
  int result = 0;
  if (*tok && IsNumber(*tok)) {
    result = atoi((*tok)->data);
    *tok = (*tok)->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return result;
}

Token *ParseIntList(Token *tok, int *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseInt(&tok);
      if (i != n - 1) {
        assert(IsComma(tok));
        tok = tok->next;
      }
    }
    assert(IsCloseCurlyBrace(tok));
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
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
    assert(IsCloseCurlyBrace(tok));
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return tok;
}

f32 ParseFloat(Token **tok) {
  f32 result = 0;
  if (*tok && IsNumber(*tok)) {
    result = (f32)atof((*tok)->data);
    *tok = (*tok)->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return result;
}

Token *ParseFloatList(Token *tok, f32 *out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseFloat(&tok);
      if (i != n - 1) {
        assert(IsComma(tok));
        tok = tok->next;
      }
    }
    assert(IsCloseCurlyBrace(tok));
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
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
        assert(IsComma(tok));
        tok = tok->next;
      } else {
        // Optional final comma
        if (IsComma(tok)) {
          tok = tok->next;
        }
      }
    }
    assert(IsCloseCurlyBrace(tok));
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return tok;
}

char *ParseString(Token **tok) {
  char *result = 0;
  if (*tok && IsString(*tok)) {
    // TODO(chogan): Where should this memory come from?
    result = (char *)malloc((*tok)->size + 1);
    strncpy(result, (*tok)->data, (*tok)->size);
    result[(*tok)->size] = '\0';
    *tok = (*tok)->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return result;
}

Token *ParseStringList(Token *tok, const char **out, int n) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    for (int i = 0; i < n; ++i) {
      out[i] = ParseString(&tok);
      if (i != n - 1) {
        assert(IsComma(tok));
        tok = tok->next;
      }
    }
    assert(IsCloseCurlyBrace(tok));
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
  }
  
  return tok;
}

void RequireNumTiers(Config *config) {
  if (config->num_tiers == 0) {
    // TODO(chogan): @errorhandling
    fprintf(stderr, "num_iters required first\n");
    exit(1);
  }
}

void RequireNumSlabs(Config *config) {
  if (config->num_slabs == 0) {
    // TODO(chogan): @errorhandling
    fprintf(stderr, "num_iters required first\n");
    exit(1);
  }
}

Token *BeginStatement(Token *tok) {
  if (tok && IsIdentifier(tok)) {
    tok = tok->next;
    if (tok && IsEqual(tok)) {
      tok = tok->next;
    } else {
      // TODO(chogan): @errorhandling
    }
  } else {
    // TODO(chogan): @errorhandling
  }

  return tok;
}

Token *EndStatement(Token *tok) {
  if (tok && IsSemicolon(tok)) {
    tok = tok->next;
  } else {
    // TODO(chogan): @errorhandling
  }

  return tok;
}

void ParseTokens(TokenList *tokens, Config *config) {

  Token *tok = tokens->head;
  while (tok) {
    ConfigVariable var = GetConfigVariable(tok);

    switch (var) {
      case ConfigVariable_NumTiers: {
        tok = BeginStatement(tok);

        int val = ParseInt(&tok);
        config->num_tiers = val;

        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_Capacities: {
        RequireNumTiers(config);
        tok = BeginStatement(tok);
        tok = ParseSizetList(tok, config->capacities, config->num_tiers);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_BlockSizes: {
        RequireNumTiers(config);
        tok = BeginStatement(tok);
        tok = ParseIntList(tok, config->block_sizes, config->num_tiers);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_NumSlabs: {
        RequireNumTiers(config);
        tok = BeginStatement(tok);
        tok = ParseIntList(tok, config->num_slabs, config->num_tiers);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_SlabUnitSizes: {
        RequireNumTiers(config);
        RequireNumSlabs(config);
        tok = BeginStatement(tok);
        tok = ParseIntListList(tok, config->slab_unit_sizes, config->num_tiers,
                               config->num_slabs);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_DesiredSlabPercentages: {
        RequireNumTiers(config);
        RequireNumSlabs(config);
        tok = BeginStatement(tok);
        tok = ParseFloatListList(tok, config->desired_slab_percentages,
                                 config->num_tiers, config->num_slabs);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_BandwidthsMbps: {
        RequireNumTiers(config);
        tok = BeginStatement(tok);
        tok = ParseFloatList(tok, config->bandwidths, config->num_tiers);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_LatenciesUs: {
        RequireNumTiers(config);
        tok = BeginStatement(tok);
        tok = ParseFloatList(tok, config->latencies, config->num_tiers);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_BufferPoolArenaPercentage: {
        tok = BeginStatement(tok);
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_BufferPool] = val;
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_MetadataArenaPercentage: {
        tok = BeginStatement(tok);
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_MetaData] = val;
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_TransferWindowArenaPercentage: {
        tok = BeginStatement(tok);
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_TransferWindow] = val;
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_TransientArenaPercentage: {
        tok = BeginStatement(tok);
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_Transient] = val;
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_MountPoints: {
        RequireNumTiers(config);
        tok = BeginStatement(tok);
        tok = ParseStringList(tok, config->mount_points, config->num_tiers);
        tok = EndStatement(tok);
        break;
      }
      case ConfigVariable_RpcServerName: {
        tok = BeginStatement(tok);
        char *val = ParseString(&tok);
        config->rpc_server_name = val;
        tok = EndStatement(tok);
        break;
      }
      default: {
        assert(!"Invalid code path\n");
        break;
      }
    }
  }
}

void ParseConfig(Arena *arena, const char *path, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  EntireFile config_file = ReadEntireFile(scratch, path);
  TokenList tokens = Tokenize(scratch, config_file);
  ParseTokens(&tokens, config);
}

}  // namespace hermes

int main(int argc, char **argv) {

  if (argc < 2) {
    fprintf(stderr, "Please pass the path to a hermes.conf file\n");
    return -1;
  }

  hermes::Config config = {};

  size_t config_memory_size = 8 * 1024;
  hermes::u8 *config_memory = (hermes::u8 *)malloc(config_memory_size);
  hermes::Arena arena = {};
  hermes::InitArena(&arena, config_memory_size, config_memory);

  hermes::ParseConfig(&arena, argv[1], &config);

  // TODO(chogan): Use glog's CHECK so this test runs in release mode
  assert(config.num_tiers == 4);
  for (int i = 0; i < config.num_tiers; ++i) {
    assert(config.capacities[i] == 50);
    assert(config.block_sizes[i] == 4);
    assert(config.num_slabs[i] == 4);

    assert(config.slab_unit_sizes[i][0] == 1);
    assert(config.slab_unit_sizes[i][1] == 4);
    assert(config.slab_unit_sizes[i][2] == 16);
    assert(config.slab_unit_sizes[i][3] == 32);

    for (int j = 0; j < config.num_slabs[i]; ++j) {
      assert(config.desired_slab_percentages[i][j] == 0.25f);
    }
  }

  assert(config.bandwidths[0] == 6000.0f);
  assert(config.bandwidths[1] == 300.0f);
  assert(config.bandwidths[2] == 150.0f);
  assert(config.bandwidths[3] == 70.0f);

  assert(config.latencies[0] == 15.0f);
  assert(config.latencies[1] == 250000.0f);
  assert(config.latencies[2] == 500000.0f);
  assert(config.latencies[3] == 1000000.0f);

  assert(config.arena_percentages[hermes::kArenaType_BufferPool] == 0.85f);
  assert(config.arena_percentages[hermes::kArenaType_MetaData] == 0.04f);
  assert(config.arena_percentages[hermes::kArenaType_Transient] == 0.03f);
  assert(config.arena_percentages[hermes::kArenaType_TransferWindow] == 0.08f);

  assert(strncmp(config.mount_points[0], "", 0) == 0);
  assert(strncmp(config.mount_points[1], "./", 2) == 0);
  assert(strncmp(config.mount_points[2], "./", 2) == 0);
  assert(strncmp(config.mount_points[3], "./", 2) == 0);
  const char expected_name[] = "sockets://localhost:8080";
  assert(strncmp(config.rpc_server_name, expected_name,
                 sizeof(expected_name)) == 0);

  // TEMP(chogan):
  for (int i = 0; i < config.num_tiers; ++i) {
    free((void *)config.mount_points[i]);
  }
  free((void *)config.rpc_server_name);

  hermes::DestroyArena(&arena);

  return 0;
}
