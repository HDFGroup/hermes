#include "balanced_mapper.h"

/**
 * Namespace declaration for cleaner code.
 */
using hermes::adapter::stdio::BalancedMapper;
using hermes::adapter::stdio::FileStruct;
using hermes::adapter::stdio::HermesStruct;

MapperReturnType BalancedMapper::map(const FileStruct& file_op) {
  LOG(INFO) << "Mapping File with offset:" << file_op.offset_
            << " and size:" << file_op.size_ << "." << std::endl;

  auto mapper_return = MapperReturnType();
  size_t size_mapped = 0;
  while (file_op.size_ > size_mapped) {
    FileStruct file;
    HermesStruct hermes;
    file.offset_ = file_op.offset_ + size_mapped;
    size_t page_index = file.offset_ / kPageSize;
    hermes.offset_ = file.offset_ % kPageSize;
    auto left_size_page = kPageSize - hermes.offset_;
    hermes.size_ = left_size_page < file_op.size_ - size_mapped
                       ? left_size_page
                       : file_op.size_ - size_mapped;

    file.size_ = hermes.size_;
    hermes.blob_name_ = std::to_string(page_index);
    mapper_return.emplace_back(file, hermes);
    size_mapped += hermes.size_;
  }
  return mapper_return;
}
