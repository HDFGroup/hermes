"""
USAGE:
    cd /path/to/hermes_config_gen
    python3 generate.py

OUTPUT:
    ../../config_client_default.h (if client)
    ../../config_server_default.h (if server)
"""

import sys, os

def create_config(path, var_name, config_path, macro_name):
    with open(path) as fp:
        yaml_config_lines = fp.read().splitlines()

    # Create the hermes config string
    string_lines = []
    string_lines.append(f"const char* {var_name} = ")
    for line in yaml_config_lines:
        line = line.replace('\"', '\\\"')
        line = line.replace('\'', '\\\'')
        string_lines.append(f"\"{line}\\n\"")
    string_lines[-1] = string_lines[-1] + ';'

    # Create the configuration
    config_lines = []
    config_lines.append(f"#ifndef HERMES_SRC_CONFIG_{macro_name}_DEFAULT_H_")
    config_lines.append(f"#define HERMES_SRC_CONFIG_{macro_name}_DEFAULT_H_")
    config_lines += string_lines
    config_lines.append(f"#endif  // HERMES_SRC_CONFIG_{macro_name}_DEFAULT_H_")

    # Persist
    config = "\n".join(config_lines)
    with open(config_path, 'w') as fp:
        fp.write(config)

create_config(
    path="../../config/hermes_client_default.yaml",
    var_name="kClientDefaultConfigStr",
    config_path="../../src/config_client_default.h",
    macro_name="CLIENT"
)

create_config(
    path="../../config/hermes_server_default.yaml",
    var_name="kServerDefaultConfigStr",
    config_path="../../src/config_server_default.h",
    macro_name="SERVER"
)

