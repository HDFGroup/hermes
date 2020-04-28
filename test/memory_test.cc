#include "test_utils.h"
#include "memory_arena.h"

using namespace hermes;

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


  Arena arena = InitArenaAndAllocate(4 * 1024);

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
