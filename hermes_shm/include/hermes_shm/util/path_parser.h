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

#ifndef HERMES_PATH_PARSER_H
#define HERMES_PATH_PARSER_H

#include <cstdlib>
#include <string>
#include <regex>
#include <list>

namespace scs {

std::string path_parser(std::string path) {
  std::smatch env_names;
  std::regex expr("\\$\\{[^\\}]+\\}");
  if (!std::regex_search(path, env_names, expr)) {
    return path;
  }
  for (auto &env_name_re : env_names) {
    std::string to_replace = std::string(env_name_re);
    std::string env_name = to_replace.substr(2, to_replace.size()-3);
    std::string env_val = env_name;
    try {
      char *ret = getenv(env_name.c_str());
      if (ret) {
        env_val = ret;
      } else {
        continue;
      }
    } catch(...) {
    }
    std::regex replace_expr("\\$\\{" + env_name + "\\}");
    path = std::regex_replace(path, replace_expr, env_val);
  }
  return path;
}

}  // namespace scs

#endif  // HERMES_PATH_PARSER_H
