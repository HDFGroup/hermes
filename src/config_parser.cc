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

#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ostream>
#include <string>

#include <glog/logging.h>

#include "hermes_types.h"
#include "utils.h"
#include "memory_management.h"
#include "config_parser.h"

// Steps to add a new configuration variable:
// 1. Add an entry to the ConfigVariable enum
// 2. Add the variable name to kConfigVariableStrings (currently the strings in
// this array must be in the same order as the corresponding entries in the
// ConfigVariable enum).
// 3. If a Parse<type> function does not exist for the variable type, implement
// it.
// 4. Add a case to ParseTokens for the new variable.
// 5. Add the new variable to the Config struct.
// 6. Add an Assert to config_parser_test.cc to test the functionality.
// 7. Set a default value in InitDefaultConfig
// 8. Add the variable with documentation to test/data/hermes.conf

namespace hermes {

// TODO(chogan): Make this work independent of declaration order
static const char *kConfigVariableStrings[ConfigVariable_Count] = {
  "unknown",
  "num_devices",
  "num_targets",
  "capacities_bytes",
  "capacities_kb",
  "capacities_mb",
  "capacities_gb",
  "block_sizes_bytes",
  "block_sizes_kb",
  "block_sizes_mb",
  "block_sizes_gb",
  "num_slabs",
  "slab_unit_sizes",
  "desired_slab_percentages",
  "bandwidths_mbps",
  "latencies_us",
  "buffer_pool_arena_percentage",
  "metadata_arena_percentage",
  "transient_arena_percentage",
  "mount_points",
  "swap_mount",
  "num_buffer_organizer_retries",
  "max_buckets_per_node",
  "max_vbuckets_per_node",
  "system_view_state_update_interval_ms",
  "rpc_server_host_file",
  "rpc_server_base_name",
  "rpc_server_suffix",
  "buffer_pool_shmem_name",
  "rpc_protocol",
  "rpc_domain",
  "rpc_port",
  "buffer_organizer_port",
  "rpc_host_number_range",
  "rpc_num_threads",
  "default_placement_policy",
  "is_shared_device",
  "buffer_organizer_num_threads",
  "default_rr_split",
  "bo_capacity_thresholds_mb",
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
          [[maybe_unused]] int items_read = fread(result.data, file_size, 1,
                                                  fstream);
          assert(items_read == 1);
          result.size = file_size;
        } else {
          LOG(FATAL) << "Arena capacity (" << arena->capacity
                     << ") too small to read file of size (" << file_size
                     << ")" << std::endl;
        }

      } else {
      }
    } else {
      FailedLibraryCall("fseek");
    }

    if (fclose(fstream) != 0) {
      FailedLibraryCall("fclose");
    }

  } else {
    FailedLibraryCall("fopen");
  }

  return result;
}

void AddTokenToList(Arena *arena, TokenList *list, TokenType type,
                    u32 line_number) {
  Token *tok = PushClearedStruct<Token>(arena);
  tok->type = type;
  tok->line = line_number;
  list->head->next = tok;
  list->head = tok;
}

inline bool BeginsComment(char c) {
  bool result = c == '#';

  return result;
}

inline bool EndOfComment(char c) {
  bool result = (c == '\n') || (c == '\r');

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
  bool result = (c >= '0' && c <= '9') || (c == '.') || (c == '-');

  return result;
}

inline bool EndOfNumber(char c) {
  bool result = (IsWhitespace(c) || (c == ',') || (c == ';') || (c == '}')
                 || (c == '-'));

  return result;
}

inline bool IsEndOfLine(char **at, char *end) {
  // Linux style
  bool result = (*at)[0] == '\n';

  // Windows style
  if (*at + 1 < end && !result) {
    result = ((*at)[0] == '\r' && (*at)[1] == '\n');
    if (result) {
      (*at)++;
    }
  }

  return result;
}

TokenList Tokenize(Arena *arena, EntireFile entire_file) {
  u32 line_number = 1;
  TokenList result = {};
  Token dummy = {};
  result.head = &dummy;

  char *at = (char *)entire_file.data;
  char *end = at + entire_file.size;

  while (at < end) {
    if (IsWhitespace(*at)) {
      if (IsEndOfLine(&at, end)) {
        line_number++;
      }

      ++at;
      continue;
    }

    if (BeginsComment(*at)) {
      while (at < end) {
        if (IsEndOfLine(&at, end)) {
          line_number++;
          ++at;
          break;
        }
        ++at;
      }
      continue;
    }

    if (BeginsIdentifier(*at)) {
      AddTokenToList(arena, &result, TokenType::Identifier, line_number);
      result.head->data = at;

      while (at && !EndOfIdentifier(*at)) {
        result.head->size++;
        at++;
      }
    } else if (BeginsNumber(*at)) {
      AddTokenToList(arena, &result, TokenType::Number, line_number);
      result.head->data = at;

      if (*at == '-') {
        result.head->size++;
        at++;
      }

      while (at && !EndOfNumber(*at)) {
        result.head->size++;
        at++;
      }
    } else {
      switch (*at) {
        case ';': {
          AddTokenToList(arena, &result, TokenType::Semicolon, line_number);
          break;
        }
        case '=': {
          AddTokenToList(arena, &result, TokenType::Equal, line_number);
          break;
        }
        case ',': {
          AddTokenToList(arena, &result, TokenType::Comma, line_number);
          break;
        }
        case '{': {
          AddTokenToList(arena, &result, TokenType::OpenCurlyBrace,
                         line_number);
          break;
        }
        case '}': {
          AddTokenToList(arena, &result, TokenType::CloseCurlyBrace,
                         line_number);
          break;
        }
        case '"': {
          AddTokenToList(arena, &result, TokenType::String, line_number);
          at++;
          result.head->data = at;

          while (at && *at != '"') {
            result.head->size++;
            at++;
          }
          break;
        }
        default: {
          LOG(FATAL) << "Config parser encountered unexpected token on line "
                     << line_number << ": " << *at << "\n";
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

inline bool IsHyphen(Token *tok) {
  bool result = tok->type == TokenType::Hyphen;

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

void PrintExpectedAndFail(const std::string &expected, u32 line_number = 0) {
  std::ostringstream msg;
  msg << "Configuration parser expected '" << expected << "'";
  if (line_number > 0) {
    msg << " on line " << line_number;
  }
  msg << "\n";

  LOG(FATAL) << msg.str();
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
    PrintExpectedAndFail("a number", (*tok)->line);
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
          PrintExpectedAndFail(",", tok->line);
        }
      }
    }

    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
  }

  return tok;
}

int ParseInt(Token **tok) {
  long result = 0;
  if (*tok && IsNumber(*tok)) {
    errno = 0;
    result = strtol((*tok)->data, NULL, 0);
    if (errno == ERANGE || (result == 0 && errno != 0) || result >= INT_MAX) {
      PrintExpectedAndFail("an integer between 0 and INT_MAX", (*tok)->line);
    }
    *tok = (*tok)->next;
  } else {
    PrintExpectedAndFail("a number", (*tok)->line);
  }

  return (int)result;
}

Token *ParseRangeList(Token *tok, std::vector<int> &host_numbers) {
  if (IsOpenCurlyBrace(tok)) {
    tok = tok->next;
    if (IsNumber(tok)) {
      while (tok) {
        int range_start = ParseInt(&tok);
        host_numbers.push_back(range_start);

        if (IsNumber(tok)) {
          // This entry in the list represents a range
          int negative_range_end = ParseInt(&tok);
          if (negative_range_end > 0) {
            PrintExpectedAndFail("a range", tok->line);
          }
          int range_end = -1 * negative_range_end;
          for (int i = range_start + 1; i <= range_end; ++i) {
            host_numbers.push_back(i);
          }
        }

        if (IsComma(tok)) {
          tok = tok->next;
        } else if (IsCloseCurlyBrace(tok)) {
          break;
        } else {
          PrintExpectedAndFail("either a range, a comma, or a }", tok->line);
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
  }

  return tok;
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
          PrintExpectedAndFail(",", tok->line);
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
  }

  return tok;
}

template<int N>
Token *ParseIntListList(Token *tok, int out[][N], int n, int *m) {
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
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
  }

  return tok;
}

f32 ParseFloat(Token **tok) {
  double result = 0;
  if (*tok && IsNumber(*tok)) {
    result = std::stod(std::string((*tok)->data), nullptr);
    *tok = (*tok)->next;
  } else {
    PrintExpectedAndFail("a number", (*tok)->line);
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
          PrintExpectedAndFail(",", tok->line);
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
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
          PrintExpectedAndFail(",", tok->line);
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
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
  }

  return tok;
}

std::string ParseString(Token **tok) {
  std::string result;
  if (*tok && IsString(*tok)) {
    result = std::string((*tok)->data, (*tok)->size);
    *tok = (*tok)->next;
  } else {
    PrintExpectedAndFail("a string", (*tok)->line);
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
          PrintExpectedAndFail(",", tok->line);
        }
      }
    }
    if (IsCloseCurlyBrace(tok)) {
      tok = tok->next;
    } else {
      PrintExpectedAndFail("}", tok->line);
    }
  } else {
    PrintExpectedAndFail("{", tok->line);
  }

  return tok;
}

Token *ParseCharArrayString(Token *tok, char *arr) {
  if (tok && IsString(tok)) {
    strncpy(arr, tok->data, tok->size);
    arr[tok->size] = '\0';
    tok = tok->next;
  } else {
    PrintExpectedAndFail("a string", tok->line);
  }

  return tok;
}

void RequireNumDevices(Config *config) {
  if (config->num_devices == 0) {
    LOG(FATAL) << "The configuration variable 'num_devices' must be defined "
               << "first" << std::endl;
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
      PrintExpectedAndFail("=", tok->line);
    }
  } else {
    PrintExpectedAndFail("an identifier", tok->line);
  }

  return tok;
}

Token *EndStatement(Token *tok) {
  if (tok && IsSemicolon(tok)) {
    tok = tok->next;
  } else {
    PrintExpectedAndFail(";", tok->line);
  }

  return tok;
}

void CheckConstraints(Config *config) {
  // rpc_domain must be present if rpc_protocol is "verbs"
  if (config->rpc_protocol.find("verbs") != std::string::npos &&
      config->rpc_domain.empty()) {
    PrintExpectedAndFail("a non-empty value for rpc_domain");
  }

  double tolerance = 0.0000001;

  // arena_percentages must add up to 1.0
  double arena_percentage_sum = 0;
  for (int i = 0; i < kArenaType_Count; ++i) {
    arena_percentage_sum += config->arena_percentages[i];
  }
  if (fabs(1.0 - arena_percentage_sum) > tolerance) {
    std::ostringstream msg;
    msg << "the values in arena_percentages to add up to 1.0 but got ";
    msg << arena_percentage_sum << "\n";
    PrintExpectedAndFail(msg.str());
  }

  // Each slab's desired_slab_percentages should add up to 1.0
  for (int device = 0; device < config->num_devices; ++device) {
    double total_slab_percentage = 0;
    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      total_slab_percentage += config->desired_slab_percentages[device][slab];
    }
    if (fabs(1.0 - total_slab_percentage) > tolerance) {
      std::ostringstream msg;
      msg << "the values in desired_slab_percentages[";
      msg << device;
      msg << "] to add up to 1.0 but got ";
      msg << total_slab_percentage << "\n";
      PrintExpectedAndFail(msg.str());
    }
  }
}

void RequireCapacitiesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Capacities are specified multiple times in the configuration"
               << " file. Only use one of 'capacities_bytes', 'capacities_kb',"
               << "'capacities_mb', or 'capacities_gb'\n";
  } else {
    already_specified = true;
  }
}

void RequireBlockSizesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Block sizes are specified multiple times in the "
               << "configuration file. Only use one of 'block_sizes_bytes',"
               << "'block_sizes_kb', 'block_sizes_mb', or 'block_sizes_gb'\n";
  } else {
    already_specified = true;
  }
}

Token *ParseCapacities(Config *config, Token *tok, bool &already_specified,
                       size_t conversion) {
  RequireNumDevices(config);
  RequireCapacitiesUnset(already_specified);
  tok = ParseSizetList(tok, config->capacities, config->num_devices);
  for (int i = 0; i < config->num_devices; ++i) {
    config->capacities[i] *= conversion;
  }

  return tok;
}

Token *ParseBlockSizes(Config *config, Token *tok, bool &already_specified,
                       size_t conversion) {
  RequireNumDevices(config);
  RequireBlockSizesUnset(already_specified);
  tok = ParseIntList(tok, config->block_sizes, config->num_devices);
  for (int i = 0; i < config->num_devices; ++i) {
    size_t val = (size_t)config->block_sizes[i] * conversion;
    if (val > INT_MAX) {
      LOG(FATAL) << "Max supported block size is " << INT_MAX << " bytes. "
                 << "Config file requested " << val << " bytes\n";
    }
    config->block_sizes[i] *= conversion;
  }

  return tok;
}

void ParseTokens(TokenList *tokens, Config *config) {
  bool capacities_specified = false;
  bool block_sizes_specified = false;

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
      case ConfigVariable_NumDevices: {
        int val = ParseInt(&tok);
        config->num_devices = val;
        // TODO(chogan): For now we set the number of Targets equal to the
        // number of Devices in order to avoid confusion, but in the future
        // we'll allow multiple Targets per Device.
        config->num_targets = val;
        break;
      }
      case ConfigVariable_NumTargets: {
        int val = ParseInt(&tok);
        config->num_targets = val;
        break;
      }
      case ConfigVariable_CapacitiesB: {
        tok = ParseCapacities(config, tok, capacities_specified, 1);
        break;
      }
      case ConfigVariable_CapacitiesKb: {
        tok = ParseCapacities(config, tok, capacities_specified, KILOBYTES(1));
        break;
      }
      case ConfigVariable_CapacitiesMb: {
        tok = ParseCapacities(config, tok, capacities_specified, MEGABYTES(1));
        break;
      }
      case ConfigVariable_CapacitiesGb: {
        tok = ParseCapacities(config, tok, capacities_specified, GIGABYTES(1));
        break;
      }
      case ConfigVariable_BlockSizesB: {
        tok = ParseBlockSizes(config, tok, block_sizes_specified, 1);
        break;
      }
      case ConfigVariable_BlockSizesKb: {
        tok = ParseBlockSizes(config, tok, block_sizes_specified, KILOBYTES(1));
        break;
      }
      case ConfigVariable_BlockSizesMb: {
        tok = ParseBlockSizes(config, tok, block_sizes_specified, MEGABYTES(1));
        break;
      }
      case ConfigVariable_BlockSizesGb: {
        tok = ParseBlockSizes(config, tok, block_sizes_specified, GIGABYTES(1));
        break;
      }
      case ConfigVariable_NumSlabs: {
        RequireNumDevices(config);
        tok = ParseIntList(tok, config->num_slabs, config->num_devices);
        break;
      }
      case ConfigVariable_SlabUnitSizes: {
        RequireNumDevices(config);
        RequireNumSlabs(config);
        tok = ParseIntListList(tok, config->slab_unit_sizes,
                               config->num_devices, config->num_slabs);
        break;
      }
      case ConfigVariable_DesiredSlabPercentages: {
        RequireNumDevices(config);
        RequireNumSlabs(config);
        tok = ParseFloatListList(tok, config->desired_slab_percentages,
                                 config->num_devices, config->num_slabs);
        break;
      }
      case ConfigVariable_BandwidthsMbps: {
        RequireNumDevices(config);
        tok = ParseFloatList(tok, config->bandwidths, config->num_devices);
        break;
      }
      case ConfigVariable_LatenciesUs: {
        RequireNumDevices(config);
        tok = ParseFloatList(tok, config->latencies, config->num_devices);
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
      case ConfigVariable_TransientArenaPercentage: {
        f32 val = ParseFloat(&tok);
        config->arena_percentages[hermes::kArenaType_Transient] = val;
        break;
      }
      case ConfigVariable_MountPoints: {
        RequireNumDevices(config);
        tok = ParseStringList(tok, config->mount_points, config->num_devices);
        break;
      }
      case ConfigVariable_SwapMount: {
        config->swap_mount = ParseString(&tok);
        break;
      }
      case ConfigVariable_NumBufferOrganizerRetries: {
        config->num_buffer_organizer_retries = ParseInt(&tok);
        break;
      }
      case ConfigVariable_MaxBucketsPerNode: {
        config->max_buckets_per_node = ParseInt(&tok);
        break;
      }
      case ConfigVariable_MaxVBucketsPerNode: {
        config->max_vbuckets_per_node = ParseInt(&tok);
        break;
      }
      case ConfigVariable_SystemViewStateUpdateInterval: {
        config->system_view_state_update_interval_ms = ParseInt(&tok);
        break;
      }
      case ConfigVariable_RpcServerHostFile: {
        config->rpc_server_host_file = ParseString(&tok);
        break;
      }
      case ConfigVariable_RpcServerBaseName: {
        config->rpc_server_base_name = ParseString(&tok);
        break;
      }
      case ConfigVariable_RpcServerSuffix: {
        config->rpc_server_suffix = ParseString(&tok);
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
      case ConfigVariable_RpcDomain: {
        config->rpc_domain = ParseString(&tok);
        break;
      }
      case ConfigVariable_RpcPort: {
        config->rpc_port = ParseInt(&tok);
        break;
      }
      case ConfigVariable_BufferOrganizerPort: {
        config->buffer_organizer_port = ParseInt(&tok);
        break;
      }
      case ConfigVariable_RpcHostNumberRange: {
        tok = ParseRangeList(tok, config->host_numbers);
        break;
      }
      case ConfigVariable_RpcNumThreads: {
        config->rpc_num_threads = ParseInt(&tok);
        break;
      }
      case ConfigVariable_PlacementPolicy: {
        std::string policy = ParseString(&tok);

        if (policy == "MinimizeIoTime") {
          config->default_placement_policy =
            api::PlacementPolicy::kMinimizeIoTime;
        } else if (policy == "Random") {
          config->default_placement_policy = api::PlacementPolicy::kRandom;
        } else if (policy == "RoundRobin") {
          config->default_placement_policy = api::PlacementPolicy::kRoundRobin;
        } else {
          LOG(FATAL) << "Unknown default_placement_policy: '" << policy << "'"
                     << std::endl;
        }
        break;
      }
      case ConfigVariable_IsSharedDevice: {
        RequireNumDevices(config);
        tok = ParseIntList(tok, config->is_shared_device, config->num_devices);
        break;
      }
      case ConfigVariable_BoNumThreads: {
        config->bo_num_threads = ParseInt(&tok);
        break;
      }
      case ConfigVariable_RRSplit: {
        config->default_rr_split = ParseInt(&tok);
        break;
      }
      case ConfigVariable_BOCapacityThresholdsMiB: {
        RequireNumDevices(config);
        // Each entry has a min and max threshold
        std::vector<int> num_thresholds(config->num_devices, 2);
        tok = ParseIntListList(tok, config->bo_capacity_thresholds_mb,
                               config->num_devices, num_thresholds.data());
        break;
      }
      default: {
        HERMES_INVALID_CODE_PATH;
        break;
      }
    }
    tok = EndStatement(tok);
  }
  CheckConstraints(config);
}

void ParseConfig(Arena *arena, const char *path, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  EntireFile config_file = ReadEntireFile(scratch, path);
  TokenList tokens = Tokenize(scratch, config_file);
  InitDefaultConfig(config);
  ParseTokens(&tokens, config);
}

}  // namespace hermes
