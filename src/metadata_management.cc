#include "metadata_management.h"

#include <string.h>

#include <string>

#include "memory_arena.h"
#include "buffer_pool.h"

namespace hermes {

void InitMetadataManager(MetadataManager *mdm, Arena *arena) {

  // list of BucketInfo
  ptrdiff_t bucket_free_list_offset;
  ptrdiff_t bucket_info_offset;
  u32 num_buckets;

  // list of VBucketInfo
  ptrdiff_t vbucket_free_list_offset;
  ptrdiff_t vbucket_info_offset;
  u32 num_vbuckets;

  // ID Heap
  ptrdiff_t blob_id_free_list_offset;
  ptrdiff_t buffer_id_free_list_offset;

  // ID Maps
  ptrdiff_t bucket_map_offset;
  ptrdiff_t vbucket_map_offset;
  ptrdiff_t blob_map_offset;

  // Mutexes
  TicketMutex bucket_mutex;
  TicketMutex vbucket_mutex;
  TicketMutex blob_id_mutex;
  TicketMutex buffer_id_mutex;

  // map mutexes?
}

MetadataManager *GetMetadataManagerFromContext(SharedMemoryContext *context) {
  MetadataManager *result =
    (MetadataManager *)(context->shm_base + contex->metadata_manager_offset);

  return result;
}

void CoalesceFreeBlocks(Heap *heap) {
  (void)heap;
}

#if 0
FreeBlock *FindBestFit(FreeBlock *head, size_t desired_size, u32 threshold=0) {
  FreeBlock *result = 0;
  FreeBlock *prev = head;
  FreeBlock *smallest_prev = head;
  u32 smallest_wasted = 0xFFFFFFFF;

  while(head && smallest_wasted > threshold) {
    if (head->size >= desired_size) {
      u32 wasted_space = head->size - desired_size ;
      if (wasted_space < smallest_wasted) {
        smallest_wasted = wasted_space;
        result = head;
        smallest_prev = prev;
      }
    }
    prev = head;
    head = head->next;
  }

  if (result) {
    smallest_prev->next = result->next;
  }

  return result;
}

String *PushString(Heap *heap, const char *str, size_t length) {
  String *result = 0;

  if (heap->free_list) {
    FreeBlock *best_fit = FindBestFit(heap->free_list,
                                      sizeof(String) + length + 1);
    if (best_fit) {
      result = (String *)best_fit;
      result->str = (char *)(result + 1);
    }
  }

  if (!result) {
    result = PushStruct<String>(&heap->arena);
    size_t total_size = RoundUpToMultiple(length + 1, heap->alignment);
    result->str = PushArray<char>(&heap->arena, total_size, 1);
  }

  result->length = length;
  for (int i = 0; i < length; ++i) {
    result->str[i] = str[i];
  }
  result->str[length] = '\0';

  return result;
}

String *PushString(Heap *heap, const char *str) {
  size_t max_size = heap->arena.capacity - heap->arena.used - sizeof(String);
  size_t length = strnlen(str, max_size - 1);
  String *result = PushString(heap, str, length);

  return result;
}

String *PushString(Heap *heap, const std::string &str) {
  String *result = PushString(heap, str.c_str(), str.size());

  return result;
}

void FreeString(Heap *heap, String **str) {
  if (heap && str && *str) {
    FreeBlock *new_block = (FreeBlock *)(*str);
    new_block->next = heap->free_list;
    size_t freed_str_size = RoundUpToMultiple((*str)->length + 1,
                                              heap->alignment);
    new_block->size = freed_str_size + sizeof(**str);
    heap->free_list = new_block;

    // Invalidate caller's pointer so they can't accidentally access freed data
    *str = 0;
  }
}
#endif
}  // namespace hermes
