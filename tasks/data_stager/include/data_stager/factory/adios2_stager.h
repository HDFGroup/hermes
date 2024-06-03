//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_Adios2_STAGER_H_
#define HERMES_TASKS_DATA_STAGER_SRC_Adios2_STAGER_H_

#include "abstract_stager.h"
#include "hermes_adapters/mapper/abstract_mapper.h"
#include <adios2.h>

namespace hermes::data_stager {

enum class DataType {
  kChar,
  kShort,
  kInt,
  kLong,
  kFloat,
  kDouble
};

class Adios2Stager : public AbstractStager {
 public:
  size_t page_size_;
  std::string config_path_;
  std::string io_name_;
  bitfield32_t flags_;
  adios2::ADIOS adios_;
  adios2::IO io_;

 public:
  /** Default constructor */
  Adios2Stager() = default;

  /** Destructor */
  ~Adios2Stager() {}

  /** Build context for staging */
  static Context BuildContext(const std::string &config_path,
                              const std::string &io_name,
                              u32 flags = 0) {
    Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
    ctx.bkt_params_ = BuildFileParams(config_path, io_name, flags);
    return ctx;
  }

  /** Build serialized file parameter pack */
  static std::string BuildFileParams(const std::string &config_path,
                                     const std::string &io_name,
                                     u32 flags = 0) {
    hshm::charbuf params(4096);
    hrun::LocalSerialize srl(params);
    srl << std::string("adios2");
    srl << flags;
    srl << config_path;
    srl << io_name;
    return params.str();
  }

  /** Create blob name */
  template<typename T>
  static std::string CreateBlobName(const adios2::Variable<T> &var) {
    hshm::charbuf params(4096);
    hrun::LocalSerialize srl(params);
    srl << var.Name();
    if constexpr (std::is_same<T, char>::value) {
      srl << DataType::kChar;
    } else if constexpr (std::is_same<T, short>::value) {
      srl << DataType::kShort;
    } else if constexpr (std::is_same<T, int>::value) {
      srl << DataType::kInt;
    } else if constexpr (std::is_same<T, long>::value) {
      srl << DataType::kLong;
    } else if constexpr (std::is_same<T, float>::value) {
      srl << DataType::kFloat;
    } else if constexpr (std::is_same<T, double>::value) {
      srl << DataType::kDouble;
    }
    SerializeDims(srl, var.Shape());
    SerializeDims(srl, var.Start());
    SerializeDims(srl, var.Count());
    return params.str();
  }

  /** Serialize adios2 dims */
  static void SerializeDims(hrun::LocalSerialize<hshm::charbuf> &srl,
                            const adios2::Dims &dims) {
    srl << dims.size();
    for (size_t dim : dims) {
      srl << dim;
    }
  }

  /** Decode blob name */
  static void DecodeBlobName(const std::string &blob_name,
                             std::string &var_name,
                             DataType &data_type,
                             adios2::Dims &shape,
                             adios2::Dims &start,
                             adios2::Dims &count) {
    hrun::LocalDeserialize srl(blob_name);
    srl >> var_name;
    srl >> data_type;
    DeserializeDims(srl, shape);
    DeserializeDims(srl, start);
    DeserializeDims(srl, count);
  }

  /** Deserialize adios2 dims */
  static void DeserializeDims(hrun::LocalDeserialize<std::string> &srl,
                              adios2::Dims &dims) {
    size_t dim_size;
    srl >> dim_size;
    dims.resize(dim_size);
    for (size_t i = 0; i < dim_size; ++i) {
      srl >> dims[i];
    }
  }

  /** Create the data stager payload */
  void RegisterStager(RegisterStagerTask *task, RunContext &rctx) override {
    std::string params = task->params_->str();
    std::string protocol;
    hrun::LocalDeserialize srl(params);
    srl >> protocol;
    srl >> flags_.bits_;
    srl >> config_path_;
    srl >> io_name_;
    path_ = task->tag_name_->str();
    adios_ = adios2::ADIOS(config_path_);
    io_ = adios_.DeclareIO(io_name_);
  }

  /** Stage data in from remote source */
  void StageIn(blob_mdm::Client &blob_mdm, StageInTask *task, RunContext &rctx) override {
    if (flags_.Any(HERMES_STAGE_NO_READ)) {
      return;
    }
    std::string blob_name = task->blob_name_->str();

    // Read blob from PFS
    try {
      std::string var_name;
      DataType data_type;
      adios2::Dims shape, start, count;
      DecodeBlobName(blob_name, var_name, data_type,
                     shape, start, count);
      adios2::Engine reader = io_.Open(path_, adios2::Mode::Read);
      LPointer<char> blob =
          HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_STD>(task->data_size_);
      switch (data_type) {
        case DataType::kChar: {
          adios2::Variable<char> var = io_.DefineVariable<char>(
              var_name, shape, start, count);
          reader.Get<char>(var, (char *) blob.ptr_, adios2::Mode::Sync);
          break;
        }
        case DataType::kShort: {
          adios2::Variable<short> var = io_.DefineVariable<short>(
              var_name, shape, start, count);
          reader.Get<short>(var, (short *) blob.ptr_, adios2::Mode::Sync);
          break;
        }
        case DataType::kInt: {
          adios2::Variable<int> var = io_.DefineVariable<int>(
              var_name, shape, start, count);
          reader.Get<int>(var, (int *) blob.ptr_, adios2::Mode::Sync);
          break;
        }
        case DataType::kLong: {
          adios2::Variable<long> var = io_.DefineVariable<long>(
              var_name, shape, start, count);
          reader.Get<long>(var, (long *) blob.ptr_, adios2::Mode::Sync);
          break;
        }
        case DataType::kFloat: {
          adios2::Variable<float> var = io_.DefineVariable<float>(
              var_name, shape, start, count);
          reader.Get<float>(var, (float *) blob.ptr_, adios2::Mode::Sync);
          break;
        }
        case DataType::kDouble: {
          adios2::Variable<double> var = io_.DefineVariable<double>(
              var_name, shape, start, count);
          reader.Get<double>(var, (double *) blob.ptr_, adios2::Mode::Sync);
          break;
        }
      }

      // Write blob to hermes
      HILOG(kDebug, "Submitting put blob {} ({}) to blob mdm ({})",
            task->blob_name_->str(), task->bkt_id_, blob_mdm.id_)
      hapi::Context ctx;
      ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
      LPointer<blob_mdm::PutBlobTask> put_task =
          blob_mdm.AsyncPutBlob(task->task_node_ + 1,
                                task->bkt_id_,
                                hshm::to_charbuf(*task->blob_name_),
                                hermes::BlobId::GetNull(),
                                0, task->data_size_, blob.shm_, task->score_, 0,
                                ctx, TASK_DATA_OWNER | TASK_LOW_LATENCY);
      put_task->Wait<TASK_YIELD_CO>(task);
      HRUN_CLIENT->DelTask(put_task);
    } catch (...) {
    }
  }

  /** Stage data out to remote source */
  void StageOut(blob_mdm::Client &blob_mdm, StageOutTask *task, RunContext &rctx) override {
    if (flags_.Any(HERMES_STAGE_NO_WRITE)) {
      return;
    }
    std::string blob_name = task->blob_name_->str();

    // Read variable info from PFS
    std::string var_name;
    DataType data_type;
    adios2::Dims shape, start, count;
    DecodeBlobName(blob_name, var_name, data_type,
                   shape, start, count);
    adios2::Engine writer = io_.Open(path_, adios2::Mode::Write);
    char *data = HRUN_CLIENT->GetDataPointer(task->data_);
    switch (data_type) {
      case DataType::kChar: {
        adios2::Variable<char> var = io_.DefineVariable<char>(
            var_name, shape, start, count);
        writer.Put<char>(var, (char*)data, adios2::Mode::Sync);
        break;
      }
      case DataType::kShort: {
        adios2::Variable<short> var = io_.DefineVariable<short>(
            var_name, shape, start, count);
        writer.Put<short>(var, (short*)data, adios2::Mode::Sync);
        break;
      }
      case DataType::kInt: {
        adios2::Variable<int> var = io_.DefineVariable<int>(
            var_name, shape, start, count);
        writer.Put<int>(var, (int*)data, adios2::Mode::Sync);
        break;
      }
      case DataType::kLong: {
        adios2::Variable<long> var = io_.DefineVariable<long>(
            var_name, shape, start, count);
        writer.Put<long>(var, (long*)data, adios2::Mode::Sync);
        break;
      }
      case DataType::kFloat: {
        adios2::Variable<float> var = io_.DefineVariable<float>(
            var_name, shape, start, count);
        writer.Put<float>(var, (float*)data, adios2::Mode::Sync);
        break;
      }
      case DataType::kDouble: {
        adios2::Variable<double> var = io_.DefineVariable<double>(
            var_name, shape, start, count);
        writer.Put<double>(var, (double*)data, adios2::Mode::Sync);
        break;
      }
    }
    HILOG(kDebug, "Staged out {} bytes to the backend file {}",
          task->data_size_, path_);
  }

  void UpdateSize(bucket_mdm::Client &bkt_mdm, UpdateSizeTask *task, RunContext &rctx) override {
    // TODO(llogan)
  }
};

}  // namespace hermes::data_stager

#endif  // HERMES_TASKS_DATA_STAGER_SRC_Adios2_STAGER_H_
