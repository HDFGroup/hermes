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

#include <string.h>

#include "hermes_types.h"

namespace hermes {

void ParseConfig(const char *path, Config *config);
void ParseConfigString(const std::string &config_string,
                       Config *config);
void InitConfig(hermes::Config *config, const char *config_file);
/** create configuration file */
hermes::Config *CreateConfig(const char *config_file);

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
