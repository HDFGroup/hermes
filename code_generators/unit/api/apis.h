//
// Created by lukemartinlogan on 12/6/22.
//

namespace hello::hi {
}  // namespace hello::hi

#ifndef HERMES_CODE_GENERATORS_CODE_GENERATORS_UNIT_PROTOS_H_
#define HERMES_CODE_GENERATORS_CODE_GENERATORS_UNIT_PROTOS_H_

#include <vector>

#define RPC
#define RPC_AUTOGEN_START
#define RPC_AUTOGEN_END

#define WRAP
#define WRAP_AUTOGEN_START
#define WRAP_AUTOGEN_END

struct Ctx {
};

int f1();

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
//
RPC int Localf2();

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */

RPC int Localf3(int a, int b);

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
template<typename T>
RPC std::vector<int>& Localf4(int a,
                         std::vector<int> b,
                         Ctx = Ctx());

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
template<typename T,
         typename S,
         class Hash = std::hash<S>>
WRAP RPC std::vector<int>& Localf5(int a,
                              std::vector<int> b,
                              Ctx = Ctx());

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
WRAP RPC
std::vector<int>& Localf6(int a, std::vector<int> b, Ctx = Ctx());

namespace nstest {

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
RPC int Localf99();


namespace nstest2 {
template <typename T>
class Hi {
  /**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC int Localf100();

  RPC_AUTOGEN_START
  int f100() {
    u32 target_node = 0;
    if (target_node == rpc_->node_id_) {
      return Localf100(
        );
    } else {
      return rpc_->Call<int>(
        target_node, "f100",
        );
    }
  }
  RPC_AUTOGEN_END
};
}  // namespace nstest2

template<typename S>
class BigHi {
    /**
     * Get or create a bucket with \a bkt_name bucket name
     *
     * @RPC_TARGET_NODE 0
     * @RPC_CLASS_INSTANCE mdm
     * */
    RPC int Localf101();

    RPC_AUTOGEN_START
    int f101() {
      u32 target_node = 0;
      if (target_node == rpc_->node_id_) {
        return Localf101(
          );
      } else {
        return rpc_->Call<int>(
          target_node, "f101",
          );
      }
    }
    RPC_AUTOGEN_END
};

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE 0
   * @RPC_CLASS_INSTANCE mdm
   * */
RPC int Localf102();

RPC_AUTOGEN_START
int f99() {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf99(
      );
  } else {
    return rpc_->Call<int>(
      target_node, "f99",
      );
  }
}
int f102() {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf102(
      );
  } else {
    return rpc_->Call<int>(
      target_node, "f102",
      );
  }
}
RPC_AUTOGEN_END
}  // namespace nstest

RPC_AUTOGEN_START
int f2() {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf2(
      );
  } else {
    return rpc_->Call<int>(
      target_node, "f2",
      );
  }
}
int f3(int a, int b) {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf3(
      a, b);
  } else {
    return rpc_->Call<int>(
      target_node, "f3",
      a, b);
  }
}
std::vector<int>& f4(int a, std::vector<int> b,  Ctx) {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf4(
      a, b);
  } else {
    return rpc_->Call<std::vector<int>&>(
      target_node, "f4",
      a, b);
  }
}
WRAP std::vector<int>& f5(int a, std::vector<int> b,  Ctx) {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf5(
      a, b);
  } else {
    return rpc_->Call<WRAP std::vector<int>&>(
      target_node, "f5",
      a, b);
  }
}
WRAP std::vector<int>& f6(int a, std::vector<int> b,  Ctx) {
  u32 target_node = 0;
  if (target_node == rpc_->node_id_) {
    return Localf6(
      a, b);
  } else {
    return rpc_->Call<WRAP std::vector<int>&>(
      target_node, "f6",
      a, b);
  }
}
RPC_AUTOGEN_END

#endif  // HERMES_CODE_GENERATORS_CODE_GENERATORS_UNIT_PROTOS_H_