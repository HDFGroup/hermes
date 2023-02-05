#ifndef HERMES_SRC_CONFIG_CLIENT_DEFAULT_H_
#define HERMES_SRC_CONFIG_CLIENT_DEFAULT_H_
const char* kClientDefaultConfigStr = 
"stop_daemon: true\n"
"path_inclusions: [\"/home\"]\n"
"path_exclusions: [\"/\"]\n"
"file_page_size: 1024KB\n"
"base_adapter_mode: kDefault\n"
"file_adapter_configs:\n"
"  - path: \"/\"\n"
"    page_size: 1MB\n"
"    mode: kDefault\n";
#endif  // HERMES_SRC_CONFIG_CLIENT_DEFAULT_H_