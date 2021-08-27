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

#include "balanced_mapper.h"

/**
 * Namespace declaration for cleaner code.
 */
using hermes::adapter::posix::BalancedMapper;
using hermes::adapter::posix::FileStruct;
using hermes::adapter::posix::HermesStruct;

MapperReturnType BalancedMapper::map(const FileStruct& file_op) {
  LOG(INFO) << "Mapping File with offset:" << file_op.offset_
            << " and size:" << file_op.size_ << "." << std::endl;

  auto mapper_return = MapperReturnType();
  size_t size_mapped = 0;
  while (file_op.size_ > size_mapped) {
    FileStruct file;
    file.file_id_ = file_op.file_id_;
    HermesStruct hermes;
    file.offset_ = file_op.offset_ + size_mapped;
    size_t page_index = file.offset_ / kPageSize;
    hermes.offset_ = file.offset_ % kPageSize;
    auto left_size_page = kPageSize - hermes.offset_;
    hermes.size_ = left_size_page < file_op.size_ - size_mapped
                       ? left_size_page
                       : file_op.size_ - size_mapped;

    file.size_ = hermes.size_;
    hermes.blob_name_ = std::to_string(page_index + 1);
    mapper_return.emplace_back(file, hermes);
    size_mapped += hermes.size_;
  }
  return mapper_return;
}
