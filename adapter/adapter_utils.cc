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

#include <sys/file.h>
#include "utils.cc"

using hermes::u8;
namespace stdfs = std::experimental::filesystem;

namespace {
const stdfs::path::value_type dot = '.';
inline bool is_dot(stdfs::path::value_type c) { return c == dot; }

inline bool is_dot(const stdfs::path& path) {
  const auto& filename = path.native();
  return filename.size() == 1 && is_dot(filename[0]);
}

inline bool is_dotdot(const stdfs::path& path) {
  const auto& filename = path.native();
  return filename.size() == 2 && is_dot(filename[0]) && is_dot(filename[1]);
}
}  // namespace

namespace hermes {
namespace adapter {

// NOTE(chogan): Back port of the C++17 standard fileystem implementation from
// gcc 9.1 to support gcc 7
stdfs::path LexicallyNormal(stdfs::path &path) {
  /*
  C++17 [fs.path.generic] p6
  - If the path is empty, stop.
  - Replace each slash character in the root-name with a preferred-separator.
  - Replace each directory-separator with a preferred-separator.
  - Remove each dot filename and any immediately following directory-separator.
  - As long as any appear, remove a non-dot-dot filename immediately followed
    by a directory-separator and a dot-dot filename, along with any immediately
    following directory-separator.
  - If there is a root-directory, remove all dot-dot filenames and any
    directory-separators immediately following them.
  - If the last filename is dot-dot, remove any trailing directory-separator.
  - If the path is empty, add a dot.
  */
  stdfs::path ret;
  // If the path is empty, stop.
  if (path.empty()) {
    return ret;
  }
  for (auto& p : path) {
    if (is_dotdot(p)) {
      if (ret.has_filename()) {
        // remove a non-dot-dot filename immediately followed by /..
        if (!is_dotdot(ret.filename())) {
          ret.remove_filename();
        } else {
          ret /= p;
        }
      } else if (!ret.has_relative_path()) {
        // remove a dot-dot filename immediately after root-directory
        if (!ret.has_root_directory()) {
          ret /= p;
        }
      } else {
        // Got a path with a relative path (i.e. at least one non-root
        // element) and no filename at the end (i.e. empty last element),
        // so must have a trailing slash. See what is before it.
        auto elem = (ret.end()--)--;
        if (elem->has_filename() && !is_dotdot(*elem)) {
          // Remove the filename before the trailing slash
          // (equiv. to ret = ret.parent_path().remove_filename())
          ret = ret.parent_path().remove_filename();

          // if (elem == ret.begin()) {
          //   ret.clear();
          // } else {
          //   ret._M_pathname.erase(elem->_M_pos);
          //   // Remove empty filename at the end:
          //   ret._M_cmpts.pop_back();
          //   // If we still have a trailing non-root dir separator
          //   // then leave an empty filename at the end:
          //   if (std::prev(elem)->_M_type() == _Type::_Filename) {
          //     elem->clear();
          //   }
          //   else {  // remove the component completely:
          //     ret._M_cmpts.pop_back();
          //   }
          // }
        } else {
          // Append the ".." to something ending in "../" which happens
          // when normalising paths like ".././.." and "../a/../.."
          ret /= p;
        }
      }
    } else if (is_dot(p))  {
      ret /= stdfs::path();
    } else {
      ret /= p;
    }
  }

  if (std::distance(ret.begin(), ret.end()) >= 2) {
    auto back = std::prev(ret.end());
    // If the last filename is dot-dot, ...
    if (back->empty() && is_dotdot(*std::prev(back))) {
      // ... remove any trailing directory-separator.
      ret = ret.parent_path();
    }
  } else if (ret.empty()) {
    // If the path is empty, add a dot.
    ret = ".";
  }

  return ret;
}

// NOTE(chogan): Backported from GCC 9
stdfs::path WeaklyCanonical(const stdfs::path& p) {
  stdfs::path result;
  if (stdfs::exists(stdfs::status(p))) {
    return stdfs::canonical(p);
  }

  stdfs::path tmp;
  auto iter = p.begin(), end = p.end();
  // find leading elements of p that exist:
  while (iter != end) {
    tmp = result / *iter;
    if (stdfs::exists(stdfs::status(tmp))) {
      stdfs::swap(result, tmp);
    } else {
      break;
    }
    ++iter;
  }
  // canonicalize:
  if (!result.empty()) {
    result = stdfs::canonical(result);
  }
  // append the non-existing elements:
  while (iter != end) {
    result /= *iter++;
  }
  // normalize:
  return LexicallyNormal(result);
}

// NOTE(chogan): Backported from GCC 9
stdfs::path WeaklyCanonical(const stdfs::path& p, std::error_code& ec) {
  stdfs::path result;
  stdfs::file_status st = stdfs::status(p, ec);
  if (exists(st)) {
    return stdfs::canonical(p, ec);
  } else if (stdfs::status_known(st)) {
    ec.clear();
  } else {
    return result;
  }

  stdfs::path tmp;
  auto iter = p.begin(), end = p.end();
  // find leading elements of p that exist:
  while (iter != end) {
    tmp = result / *iter;
    st = stdfs::status(tmp, ec);
    if (exists(st)) {
      swap(result, tmp);
    } else {
      if (stdfs::status_known(st)) {
        ec.clear();
      }
      break;
    }
    ++iter;
  }
  // canonicalize:
  if (!ec && !result.empty()) {
    result = canonical(result, ec);
  }
  if (ec) {
    result.clear();
  } else {
    // append the non-existing elements:
    while (iter != end) {
      result /= *iter++;
    }
    // normalize:
    result = LexicallyNormal(result);
  }

  return result;
}

void ReadGap(const std::string &filename, size_t seek_offset, u8 *read_ptr,
             size_t read_size, size_t file_bounds) {
  if (stdfs::exists(filename) &&
      stdfs::file_size(filename) >= file_bounds) {
    LOG(INFO) << "Blob has a gap in write. Read gap from original file.\n";
    INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd) {
      if (flock(fd, LOCK_SH) == -1) {
        hermes::FailedLibraryCall("flock");
      }

      ssize_t bytes_read = pread(fd, read_ptr, read_size, seek_offset);
      if (bytes_read == -1 || (size_t)bytes_read != read_size) {
        hermes::FailedLibraryCall("pread");
      }

      if (flock(fd, LOCK_UN) == -1) {
        hermes::FailedLibraryCall("flock");
      }

      if (close(fd) != 0) {
        hermes::FailedLibraryCall("close");
      }
    } else {
      hermes::FailedLibraryCall("open");
    }
    INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  }
}

}  // namespace adapter
}  // namespace hermes
