//
// Created by lukemartinlogan on 1/1/23.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_BITFIELD_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_BITFIELD_H_

#include <cstdint>

namespace hermes_shm {

#define BIT_OPT(T, n) (((T)1) << n)

/**
 * A generic bitfield template
 * */
template<typename T=uint32_t>
struct bitfield {
  T bits_;

  bitfield() : bits_(0) {}

  inline void SetBits(T mask) {
    bits_ |= mask;
  }

  inline void UnsetBits(T mask) {
    bits_ &= ~mask;
  }

  inline bool OrBits(T mask) const {
    return bits_ & mask;
  }

  inline void CopyBits(bitfield field, T mask) {
    bits_ &= (field.bits_ & mask);
  }

  inline void Clear() {
    bits_ = 0;
  }
} __attribute__((packed));
typedef bitfield<uint8_t> bitfield8_t;
typedef bitfield<uint16_t> bitfield16_t;
typedef bitfield<uint32_t> bitfield32_t;

#define INHERIT_BITFIELD_OPS(BITFIELD_VAR, MASK_T)\
  inline void SetBits(MASK_T mask) {\
    BITFIELD_VAR.SetBits(mask);\
  }\
  inline void UnsetBits(MASK_T mask) {\
    BITFIELD_VAR.UnsetBits(mask);\
  }\
  inline bool OrBits(MASK_T mask) const {\
    return BITFIELD_VAR.OrBits(mask);\
  }\
  inline void Clear() {\
    BITFIELD_VAR.Clear();\
  }\
  template<typename BITFIELD_T>\
  inline void CopyBits(BITFIELD_T field, MASK_T mask) {\
    BITFIELD_VAR.CopyBits(field, mask);\
  }

}  // namespace hermes_shm

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_BITFIELD_H_
