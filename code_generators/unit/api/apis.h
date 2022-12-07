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
RPC_AUTOGEN_END
}  // namespace nstest

RPC_AUTOGEN_START
RPC_AUTOGEN_END

#endif  // HERMES_CODE_GENERATORS_CODE_GENERATORS_UNIT_PROTOS_H_
