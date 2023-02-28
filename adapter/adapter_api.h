//
// Created by luke on 2/28/23.
//

#ifndef HERMES_ADAPTER_API_H
#define HERMES_ADAPTER_API_H

#include <dlfcn.h>
#include <link.h>

namespace hermes::adapter {

struct RealApiIter {
  const char *symbol_name_;
  const char *is_intercepted_;
  const char *lib_path_;

  RealApiIter(const char *symbol_name, const char *is_intercepted)
      : symbol_name_(symbol_name), is_intercepted_(is_intercepted),
        lib_path_(nullptr) {}
};

struct RealApi {
  void *real_lib_next_;
  void *real_lib_default_;
  bool is_intercepted_;

  static int callback(struct dl_phdr_info *info, size_t size, void *data) {
    auto iter = (RealApiIter*)data;
    auto lib = dlopen(info->dlpi_name, RTLD_GLOBAL | RTLD_NOW);
    auto exists = dlsym(lib, iter->symbol_name_);
    void *is_intercepted =
        (void*)dlsym(lib, "posix_intercepted");
    if (!is_intercepted && exists) {
      iter->lib_path_ = info->dlpi_name;
    }
    return 0;
  }

  RealApi(const char *symbol_name,
          const char *is_intercepted)
      : real_lib_next_(nullptr), real_lib_default_(nullptr) {
    RealApiIter iter(symbol_name, is_intercepted);
    dl_iterate_phdr(callback, (void*)&iter);
    if (iter.lib_path_) {
      real_lib_next_ = dlopen(iter.lib_path_, RTLD_GLOBAL | RTLD_NOW);
      real_lib_default_ = real_lib_next_;
    }
    void *is_intercepted_ptr = (void*)dlsym(RTLD_DEFAULT,
                                             is_intercepted);
    is_intercepted_ = is_intercepted_ptr != nullptr;
  }
};

}  // namespace hermes::adapter

#endif  // HERMES_ADAPTER_API_H
