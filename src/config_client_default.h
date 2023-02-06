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

#ifndef HERMES_SRC_CONFIG_CLIENT_DEFAULT_H_
#define HERMES_SRC_CONFIG_CLIENT_DEFAULT_H_
const char* kClientDefaultConfigStr = 
"stop_daemon: false\n"
"path_inclusions: [\"/home\"]\n"
"path_exclusions: [\"/\"]\n"
"file_page_size: 1024KB\n"
"base_adapter_mode: kDefault\n"
"file_adapter_configs:\n"
"  - path: \"/\"\n"
"    page_size: 1MB\n"
"    mode: kDefault\n";
#endif  // HERMES_SRC_CONFIG_CLIENT_DEFAULT_H_
