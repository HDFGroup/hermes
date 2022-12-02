"""
USAGE:
    cd /path/to/default_config_gen
    ./config_gen [client/server]

OUTPUT:
    ../../config_client_default.h (if client)
    ../../config_server_default.h (if server)
"""

import sys, os

if len(sys.argv) != 2:
    print("USAGE: ./config_gen [client/server]")
    exit(1)

config_type = sys.argv[1]
if config_type == 'client':
    path = "../hermes_client_default.yaml"
    var_name = "kClientDefaultConfigStr"
    config_path = "../../src/config_client_default.h"
    macro_name = "CLIENT"
elif config_type == 'server':
    path = "../hermes_server_default.yaml"
    var_name = "kServerDefaultConfigStr"
    config_path = "../../src/config_server_default.h"
    macro_name = "SERVER"
else:
    print(f"{config_type} is not either \"client\" or \"server\"")
    exit(1)

with open(path) as fp:
    yaml_config_lines = fp.read().splitlines()

# Create the hermes config string
string_lines = []
string_lines.append(f"const char* {var_name} = ")
for line in yaml_config_lines:
    line = line.replace('\"', '\\\"')
    line = line.replace('\'', '\\\'')
    string_lines.append(f"\"{line}\"")
string_lines[-1] = string_lines[-1] + ';'
final_string = "\n".join(string_lines)

# Create the configuration
config_lines = []
config_lines.append(f"#ifndef HERMES_SRC_CONFIG_{macro_name}_DEFAULT_H_")
config_lines.append(f"#define HERMES_SRC_CONFIG_{macro_name}_DEFAULT_H_")
config_lines.append(final_string)
config_lines.append(f"#endif  // HERMES_SRC_CONFIG_{macro_name}_DEFAULT_H_")
config = "\n".join(config_lines)
with open(config_path, 'w') as fp:
    fp.write(config)