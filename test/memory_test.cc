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

#include "test_utils.h"
#include "memory_management.h"

using namespace hermes;  // NOLINT(*)

const int kNumVals = 8;

struct SixtyFourBytes {
  u64 vals[kNumVals];
};

void FillArrayU64(SixtyFourBytes *s, u64 val) {
  for (int i = 0; i < kNumVals; ++i) {
    s->vals[i] = val;
  }
}

void CheckArrayU64(SixtyFourBytes *s, u64 val) {
  for (int i = 0; i < kNumVals; ++i) {
    Assert(s->vals[i] == val);
  }
}
void TestFillFreeBlocks(Heap *heap) {
  SixtyFourBytes *s1 = HeapPushStruct<SixtyFourBytes>(heap);
  FillArrayU64(s1, 1);
  SixtyFourBytes *s2 = HeapPushStruct<SixtyFourBytes>(heap);
  FillArrayU64(s2, 2);
  SixtyFourBytes *s3 = HeapPushStruct<SixtyFourBytes>(heap);
  FillArrayU64(s3, 3);
  SixtyFourBytes *s4 = HeapPushStruct<SixtyFourBytes>(heap);
  FillArrayU64(s4, 4);

  CheckArrayU64(s1, 1);
  CheckArrayU64(s2, 2);
  CheckArrayU64(s3, 3);
  CheckArrayU64(s4, 4);

  HeapFree(heap, s2);
  HeapFree(heap, s3);

  CheckArrayU64(s1, 1);
  CheckArrayU64(s4, 4);

  SixtyFourBytes *s5 = HeapPushStruct<SixtyFourBytes>(heap);
  FillArrayU64(s5, 5);
  SixtyFourBytes *s6 = HeapPushStruct<SixtyFourBytes>(heap);
  FillArrayU64(s6, 6);

  CheckArrayU64(s1, 1);
  CheckArrayU64(s4, 4);
  CheckArrayU64(s5, 5);
  CheckArrayU64(s6, 6);

  HeapFree(heap, s1);
  HeapFree(heap, s4);
  HeapFree(heap, s5);
  HeapFree(heap, s6);

  SixtyFourBytes *s7 = HeapPushArray<SixtyFourBytes>(heap, 2);
  FillArrayU64(s7, 7);
  FillArrayU64(s7 + 1, 7);

  CheckArrayU64(s7, 7);
  CheckArrayU64(s7 + 1, 7);

  HeapFree(heap, s7);
}

int main() {
  Arena arena = InitArenaAndAllocate(KILOBYTES(4));

  TemporaryMemory temporary_memory = BeginTemporaryMemory(&arena);
  Heap *heap = InitHeapInArena(temporary_memory.arena);
  TestFillFreeBlocks(heap);
  EndTemporaryMemory(&temporary_memory);

  temporary_memory = BeginTemporaryMemory(&arena);
  Heap *reverse_heap = InitHeapInArena(temporary_memory.arena, false);
  TestFillFreeBlocks(reverse_heap);
  EndTemporaryMemory(&temporary_memory);

  DestroyArena(&arena);

  return 0;
}
