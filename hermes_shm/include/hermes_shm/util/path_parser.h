/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#ifndef HERMES_SHM_PATH_PARSER_H
#define HERMES_SHM_PATH_PARSER_H

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

#endif  // HERMES_SHM_PATH_PARSER_H
