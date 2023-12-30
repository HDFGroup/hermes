//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "hermes_data_op/hermes_data_op.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"

namespace hermes::data_op {

struct OpPendingData {
  std::list<OpData> pending_;
  u32 num_refs_;
  u64 data_id_;

  OpPendingData() : num_refs_(0) {}
};

class Server : public TaskLib {
 public:
  std::vector<std::list<OpGraph>> op_graphs_;
  std::unordered_map<std::string, u32> op_id_map_;
  Mutex op_data_lock_;
  std::unordered_map<BucketId, OpPendingData> op_data_map_;
  hermes::bucket_mdm::Client bkt_mdm_;
  hermes::blob_mdm::Client blob_mdm_;
  Client client_;
  LPointer<RunOpTask> run_task_;

 public:
  Server() = default;

  /** Construct data operator table */
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->Deserialize();
    bkt_mdm_.Init(task->bkt_mdm_, HRUN_ADMIN->queue_id_);
    blob_mdm_.Init(task->blob_mdm_, HRUN_ADMIN->queue_id_);
    client_.Init(id_, HRUN_ADMIN->queue_id_);
    op_id_map_["min"] = 0;
    op_id_map_["max"] = 1;
    op_graphs_.resize(HRUN_QM_RUNTIME->max_lanes_);
    run_task_ = client_.AsyncRunOp(task->task_node_ + 1);
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy data operators */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** Registor operators */
  void RegisterOp(RegisterOpTask *task, RunContext &rctx) {
    // Load OpGraph
    op_graphs_[rctx.lane_id_].push_back(task->GetOpGraph());
    OpGraph &op_graph = op_graphs_[rctx.lane_id_].back();

    // Get or create all needed bucket IDs
    std::vector<TraitId> traits;
    for (Op &op : op_graph.ops_) {
      // Spawn bucket ID task for each input
      for (OpBucketName &bkt_name : op.in_) {
        bkt_name.bkt_id_task_ =
            bkt_mdm_.AsyncGetOrCreateTag(task->task_node_ + 1,
                                         hshm::charbuf(bkt_name.url_),
                                         true,
                                         traits, 0, 0);
      }
      // Spawn bucket ID task for the output
      op.var_name_.bkt_id_task_ =
          bkt_mdm_.AsyncGetOrCreateTag(task->task_node_ + 1,
                                       hshm::charbuf(op.var_name_.url_),
                                       true,
                                       traits, 0, 0);
      // Set the op ID
      op.op_id_ = op_id_map_[op.op_name_];
    }

    // Complete all bucket ID tasks
    for (Op &op : op_graph.ops_) {
      // Spawn bucket ID task for each input
      for (OpBucketName &bkt_name : op.in_) {
        bkt_name.bkt_id_task_->Wait<TASK_YIELD_CO>(task);
        bkt_name.bkt_id_ = bkt_name.bkt_id_task_->tag_id_;
        op_data_map_.emplace(bkt_name.bkt_id_, OpPendingData());
        HRUN_CLIENT->DelTask(bkt_name.bkt_id_task_);
      }
      // Spawn bucket ID task for the output
      op.var_name_.bkt_id_task_->Wait<TASK_YIELD_CO>(task);
      op.var_name_.bkt_id_ = op.var_name_.bkt_id_task_->tag_id_;
      op_data_map_.emplace(op.var_name_.bkt_id_, OpPendingData());
      HRUN_CLIENT->DelTask(op.var_name_.bkt_id_task_);
    }

    // Get number of operations that depend on each data object
    for (Op &op : op_graph.ops_) {
      for (OpBucketName &bkt_name : op.in_) {
        op_data_map_[bkt_name.bkt_id_].num_refs_++;
      }
    }

    // Store the operator to perform
    task->SetModuleComplete();
  }
  void MonitorRegisterOp(u32 mode, RegisterOpTask *task, RunContext &rctx) {
  }

  /** Inform that data is ready for operators */
  void RegisterData(RegisterDataTask *task, RunContext &rctx) {
    if (!op_data_lock_.TryLock(0)) {
      return;
    }
    OpPendingData &op_data = op_data_map_[task->data_.bkt_id_];
    task->data_.data_id_ = op_data.data_id_++;
    op_data.pending_.emplace_back(task->data_);
    op_data_lock_.Unlock();
    task->SetModuleComplete();
  }
  void MonitorRegisterData(u32 mode, RegisterDataTask *task, RunContext &rctx) {
  }

  /** Run the operator */
  void RunOp(RunOpTask *task, RunContext &rctx) {
    for (OpGraph &op_graph : op_graphs_[rctx.lane_id_]) {
      for (Op &op : op_graph.ops_) {
        switch(op.op_id_) {
          case 0:
            RunMin(task, op);
            break;
          case 1:
            RunMax(task, op);
            break;
        }
      }
    }
  }
  void MonitorRunOp(u32 mode, RunOpTask *task, RunContext &rctx) {
  }

  /** Get pending data needed for the operator */
  std::list<OpData> GetPendingData(Op &op) {
    std::list<OpData> pending;
    if (!op_data_lock_.TryLock(0)) {
      return pending;
    }
    for (OpBucketName &bkt_name : op.in_) {
      if (op_data_map_.find(bkt_name.bkt_id_) == op_data_map_.end()) {
        continue;
      }
      OpPendingData &op_data = op_data_map_[bkt_name.bkt_id_];
      pending = op_data.pending_;
      std::list<OpData> pruned;
      for (auto iter = pending.begin(); iter != pending.end(); ++iter) {
        OpData &data = *iter;
        ++data.refcnt_;
        if (data.refcnt_ < op_data.num_refs_) {
          pruned.push_back(data);
        }
        op_data.pending_ = pruned;
      }
    }
    op_data_lock_.Unlock();
    return pending;
  }

  /** Get the minimum of data */
  void RunMin(RunOpTask *task, Op &op) {
    // Get the pending data from Hermes
    std::list<OpData> op_data = GetPendingData(op);
    if (op_data.empty()) {
      return;
    }

    // Get the input data from Hermes
    typedef std::tuple<OpData&,
                       LPointer<char>,
                       LPointer<blob_mdm::GetBlobTask>> DataPair;
    std::vector<DataPair> in_tasks;
    for (OpData &data : op_data) {
      // Get the input data
      LPointer<char> data_ptr =
          HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_CO>(data.size_, task);
      LPointer<blob_mdm::GetBlobTask> in_task =
          blob_mdm_.AsyncGetBlob(task->task_node_ + 1,
                                 data.bkt_id_,
                                 hshm::charbuf(""), data.blob_id_,
                                 data.off_, data.size_,
                                 data_ptr.shm_);
      in_tasks.emplace_back(data, data_ptr, in_task);
    }

    // Calculate the min of input data
    for (DataPair &data_pair : in_tasks) {
      // Wait for data to be available
      OpData &data = std::get<0>(data_pair);
      LPointer<char> &data_ptr = std::get<1>(data_pair);
      LPointer<blob_mdm::GetBlobTask> &in_task = std::get<2>(data_pair);
      in_task->Wait<TASK_YIELD_CO>(task);

      // Calaculate the minimum
      LPointer<char> min_lptr =
          HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_CO>(sizeof(float), task);
      float *min_ptr = (float*)min_lptr.ptr_;
      *min_ptr = std::numeric_limits<float>::max();
      for (size_t i = 0; i < in_task->data_size_; i += sizeof(float)) {
        *min_ptr = std::min(*min_ptr, *(float*)(data_ptr.ptr_ + i));
      }

      // Store the minimum in Hermes
      std::string min_blob_name = data.blob_name_;
      blob_mdm_.AsyncPutBlob(task->task_node_ + 1,
                             op.var_name_.bkt_id_,
                             hshm::charbuf(min_blob_name),
                             BlobId::GetNull(),
                             0, sizeof(float),
                             min_lptr.shm_, 0, 0);
      HRUN_CLIENT->FreeBuffer(in_task->data_);
      HRUN_CLIENT->DelTask(in_task);
    }
  }

  /** Get the maximum of data */
  void RunMax(RunOpTask *task, Op &op) {
  }

 public:
#include "hermes_data_op/hermes_data_op_lib_exec.h"
};

}  // namespace hermes::data_op

HRUN_TASK_CC(hermes::data_op::Server, "hermes_data_op");
