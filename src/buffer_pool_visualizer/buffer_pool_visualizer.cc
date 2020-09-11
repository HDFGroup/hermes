#include <unordered_map>

#include "SDL.h"

#include "hermes_types.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "metadata_management.h"
#include "debug_state.h"
#include "utils.h"

namespace hermes {
Heap *GetIdHeap(MetadataManager *mdm);
Heap *GetMapHeap(MetadataManager *mdm);
}  // namespace hermes

using namespace hermes;  // NOLINT(*)

enum Color {
  kColor_Red,
  kColor_Yellow,
  kColor_Green,
  kColor_Cyan,
  kColor_Magenta,

  kColor_HeapMax,

  kColor_Grey,
  kColor_White,
  kColor_Black,

  kColor_Count
};

enum class ActiveSegment {
  BufferPool,
  Metadata,
};

enum class ActiveHeap {
  Map,
  Id,
};

static u32 global_colors[kColor_Count];
static int global_bitmap_index;
static DeviceID global_active_device;
static ActiveSegment global_active_segment;
static ActiveHeap global_active_heap;
static u32 global_color_counter;
static DebugState *id_debug_state;
static DebugState *map_debug_state;


struct Range {
  int start;
  int end;
};

struct Point {
  int x;
  int y;
};

struct HeapMetadata {
  u8 *heap_base;
  ptrdiff_t heap_size;
  int total_slots;
  int screen_width;
  int num_rows;
  int y_offset;
  int h;
  f32 slots_to_bytes;
  f32 bytes_to_slots;
};

static SDL_Window *CreateWindow(int width, int height) {
  SDL_Window *result = SDL_CreateWindow("BufferPool Visualizer",
                                        SDL_WINDOWPOS_UNDEFINED,
                                        SDL_WINDOWPOS_UNDEFINED,
                                        width,
                                        height,
                                        0 /* SDL_WINDOW_OPENGL */);

  if (result == NULL) {
    SDL_Log("Could not create window: %s\n", SDL_GetError());
    exit(1);
  }

  return result;
}

static SDL_Surface *GetScreen(SDL_Window *window) {
  SDL_Surface *result = SDL_GetWindowSurface(window);

  if (result == NULL) {
    SDL_Log("Could not get window surface: %s\n", SDL_GetError());
    exit(1);
  }

  return result;
}

static SDL_Surface *CreateBackBuffer(int width, int height) {
  Uint32 rmask;
  Uint32 gmask;
  Uint32 bmask;
  Uint32 amask;

#if SDL_BYTEORDER == SDL_BIG_ENDIAN
  rmask = 0xff000000;
  gmask = 0x00ff0000;
  bmask = 0x0000ff00;
  amask = 0x000000ff;
#else
  rmask = 0x000000ff;
  gmask = 0x0000ff00;
  bmask = 0x00ff0000;
  amask = 0xff000000;
#endif

  SDL_Surface *result = SDL_CreateRGBSurface(0, width, height, 32, rmask,
                                             gmask, bmask, amask);
  if (result == NULL) {
    SDL_Log("SDL_CreateRGBSurface() failed: %s", SDL_GetError());
    exit(1);
  }

  return result;
}

void DrawWrappingRect(SDL_Rect *rect, int width, int pad, SDL_Surface *surface,
                      u32 color) {
  int x = rect->x;
  int y = rect->y;
  int w = rect->w;
  int h = rect->h;
  bool multi_line = false;

  while (w > 0) {
    SDL_Rect fill_rect = {x + pad, y + pad, 0, h - pad};
    int rect_width = x + w;
    if (rect_width > width) {
      multi_line = true;
      int this_line_width = width - (x + pad);
      fill_rect.w = this_line_width;
      w -= this_line_width;
      x = 0;
      y += h;
    } else {
      fill_rect.w = multi_line ? w - 2 * pad : w - pad;
      w = -1;
    }
    SDL_FillRect(surface, &fill_rect, color);
  }
}

// NOTE(chogan): This won't work if we allow non-ram headers to be dormant
// because getting the next dormant header can swap headers out of their preset
// index range
static Range GetHeaderIndexRange(SharedMemoryContext *context,
                                 DeviceID device_id) {
  Range result = {};
  BufferPool *pool = GetBufferPoolFromContext(context);

  for (int i = 0; i < device_id; ++i) {
    result.start += pool->num_headers[i];
  }
  result.end = result.start + pool->num_headers[device_id];

  return result;
}

static int DrawBufferPool(SharedMemoryContext *context,
                          SDL_Surface *surface, int window_width,
                          int window_height, DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  size_t block_size = pool->block_sizes[device_id];
  int block_width_pixels = 5;
  f32 memory_offset_to_pixels = (f32)(block_width_pixels) / (f32)(block_size);
  int pad = 2;
  int h = 5;
  int x = 0;
  int y = 0;
  int w = 0;
  int final_y = 0;

  std::unordered_map<std::string, int> block_refs;

  Range header_range = GetHeaderIndexRange(context, device_id);

  for (int i = header_range.start; i < header_range.end; ++i) {
    BufferHeader *header = headers + i;
    assert((u8 *)header < (u8 *)pool);
    if (HeaderIsDormant(header)) {
      continue;
    }

    // NOTE(chogan): Mark this region as in use so we can later ensure no more
    // than one header ever refers to the same memory region
    int index = header->data_offset / block_size;
    int num_blocks = header->capacity / block_size;

    for (int j = 0; j < num_blocks; ++j) {
      std::string index_str = std::to_string(index) + std::to_string(j);
      auto found = block_refs.find(index_str);
      if (found != block_refs.end()) {
        block_refs[index_str] += 1;
      } else {
        block_refs[index_str] = 1;
      }
    }

    int pixel_offset = (int)(header->data_offset * memory_offset_to_pixels);

    y = (pixel_offset / window_width) * h;
    if (y + h + pad > window_height) {
      SDL_Log("Not enough room to display all buffers\n");
      break;
    }

    int color_index = -1;
    for (int j = 0; j < pool->num_slabs[device_id]; ++j) {
      if (num_blocks == GetSlabUnitSize(context, device_id, j)) {
        color_index = j;
        break;
      }
    }

    assert(color_index >= 0);
    u32 rgb_color = global_colors[color_index];

    if (header->in_use) {
      // TODO(chogan): Just draw used in white, not whole capacity
      rgb_color = global_colors[kColor_White];
    }

    x = pixel_offset % window_width;
    w = num_blocks * block_width_pixels;

    SDL_Rect rect = {x, y, w, h};
    DrawWrappingRect(&rect, window_width, pad, surface, rgb_color);

    if (y + pad >= final_y) {
      final_y = y + pad;
    }
  }

  // NOTE(chogan): Ensure that we are not drawing any portion of a buffer in
  // more than one place (i.e, no headers point to overlapping buffers)
  for (auto iter = block_refs.begin(); iter != block_refs.end(); ++iter) {
    if (device_id == 0) {
      assert(iter->second <= 1);
    } else {
      // TODO(chogan): Data offsets for file buffers are allowed to overlap
      // becuase each slab starts at 0
    }
  }

  return final_y + h;
}

static int DrawFileBuffers(SharedMemoryContext *context, SDL_Surface *surface,
                           int window_width, int window_height,
                           DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  size_t block_size = pool->block_sizes[device_id];
  int block_width_pixels = 5;
  f32 memory_offset_to_pixels = (f32)(block_width_pixels) / (f32)(block_size);
  int pad = 2;
  int h = 5;
  int x = 0;
  int y = 0;
  int w = 0;
  int final_y = 0;
  int starting_y_offset = 0;

  using BlockMap = std::unordered_map<std::string, int>;
  std::vector<BlockMap> block_refs(pool->num_slabs[device_id]);
  int block_refs_index = 0;

  Range header_range = GetHeaderIndexRange(context, device_id);

  for (int i = header_range.start; i < header_range.end; ++i) {
    BufferHeader *header = headers + i;
    assert((u8 *)header < (u8 *)pool);
    if (HeaderIsDormant(header)) {
      continue;
    }

    int pixel_offset = (int)(header->data_offset * memory_offset_to_pixels);

    if (pixel_offset == 0 && y) {
      starting_y_offset = final_y + h;
      block_refs_index++;
    }

    // NOTE(chogan): Mark this region as in use so we can later ensure no more
    // than one header ever refers to the same memory region
    int index = header->data_offset / block_size;
    int num_blocks = header->capacity / block_size;

    for (int j = 0; j < num_blocks; ++j) {
      std::string index_str = std::to_string(index) + std::to_string(j);
      auto found = block_refs[block_refs_index].find(index_str);
      if (found != block_refs[block_refs_index].end()) {
        block_refs[block_refs_index][index_str] += 1;
      } else {
        block_refs[block_refs_index][index_str] = 1;
      }
    }

    y = starting_y_offset + (pixel_offset / window_width) * h;
    if (y + h + pad > window_height) {
      SDL_Log("Not enough room to display all buffers\n");
      break;
    }

    int color_index = -1;
    for (int j = 0; j < pool->num_slabs[device_id]; ++j) {
      if (num_blocks == GetSlabUnitSize(context, device_id, j)) {
        color_index = j;
        break;
      }
    }

    assert(color_index >= 0);
    u32 rgb_color = global_colors[color_index];

    if (header->in_use) {
      // TODO(chogan): Just draw used in white, not whole capacity
      rgb_color = global_colors[kColor_White];
    }

    x = pixel_offset % window_width;
    w = num_blocks * block_width_pixels;

    SDL_Rect rect = {x, y, w, h};
    DrawWrappingRect(&rect, window_width, pad, surface, rgb_color);

    if (y + pad >= final_y) {
      final_y = y + pad;
    }
  }

  // NOTE(chogan): Ensure that we are not drawing any portion of a buffer in
  // more than one place (i.e, no headers point to overlapping buffers)
  for (size_t i = 0; i < block_refs.size(); ++i) {
    for (auto iter = block_refs[i].begin();
         iter != block_refs[i].end();
         ++iter) {
      assert(iter->second <= 1 && iter->second >= 0);
    }
  }

  return final_y + h;
}

static void DrawEndOfRamBuffers(SharedMemoryContext *context,
                                SDL_Surface *surface, int window_width,
                                int window_height) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  size_t block_size = pool->block_sizes[0];
  int block_width = 5;
  int w = block_width * 2;
  int h = 5;
  int pad = 2;
  f32 memory_offset_to_pixels = (f32)(block_width) / (f32)(block_size);

  int pixel_offset = (int)(pool->headers_offset * memory_offset_to_pixels);
  int x = pixel_offset % window_width;
  int y = (pixel_offset / window_width) * h;

  if (y + pad < window_height) {
    SDL_Rect rect = {x + pad, y + pad, w - pad, h - pad};
    u32 color = global_colors[kColor_Magenta];
    SDL_FillRect(surface, &rect, color);
  }
}

static void DrawHeaders(SharedMemoryContext *context, SDL_Surface *surface,
                        int window_width, int window_height, int starting_y,
                        DeviceID device_id) {
  [[maybe_unused]] BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  int pad = 1;
  int x = 0;
  int w = 2;
  int h = 5;
  int y = starting_y;


  Range index_range = GetHeaderIndexRange(context, device_id);

  for (int i = index_range.start; i < index_range.end; ++i) {
    BufferHeader *header = headers + i;
    assert((u8 *)header < (u8 *)pool);

    if (x + w + (2 * pad) > window_width) {
      y += h + pad;
      x = 0;
    }

    if (y + h + pad > window_height) {
      SDL_Log("Not enough room to display everything\n");
      break;
    }

    SDL_Rect rect = {x + pad, y + pad, w - pad, h - pad};
    u32 color = 0;
    if (HeaderIsDormant(header)) {
      color = global_colors[kColor_Grey];
    } else {
      color = global_colors[kColor_Green];
    }

    SDL_FillRect(surface, &rect, color);

    x += (2 * pad) + w;
  }
}

static void ReloadSharedMemory(SharedMemoryContext *context, char *shmem_name) {
  ReleaseSharedMemoryContext(context);
  SharedMemoryContext new_context = GetSharedMemoryContext(shmem_name);
  if (new_context.shm_base == 0) {
    SDL_Log("Couldn't open BufferPool shared memory\n");
    exit(1);
  }
  *context = new_context;
}

static void PrintBufferCounts(SharedMemoryContext *context,
                              DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  std::vector<int> buffer_counts(pool->num_slabs[device_id], 0);

  Range index_range = GetHeaderIndexRange(context, device_id);

  for (int header_index = index_range.start;
       header_index < index_range.end;
       ++header_index) {
    BufferHeader *header = headers + header_index;
    if (HeaderIsDormant(header)) {
      continue;
    }

    for (size_t i = 0; i < buffer_counts.size(); ++i) {
      if (header->capacity == (u32)GetSlabBufferSize(context, device_id, i)) {
        buffer_counts[i]++;
      }
    }
  }

  int total_headers = 0;
  for (size_t i = 0; i < buffer_counts.size(); ++i) {
    int this_count = buffer_counts[i];
    SDL_Log("Slab %d: %d\n", (int)i, this_count);
    total_headers += this_count;
  }
  SDL_Log("Total live headers: %d\n", total_headers);
}

static void PrintFreeListSizes(SharedMemoryContext *context,
                               DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  int total_free_headers = 0;

  for (int slab = 0; slab < pool->num_slabs[device_id]; ++slab) {
    BufferID next_free = PeekFirstFreeBufferId(context, device_id, slab);
    int this_slab_free = 0;
    while (next_free.as_int != 0) {
      BufferHeader *header = GetHeaderByBufferId(context, next_free);
      this_slab_free += 1;
      next_free = header->next_free;
    }
    SDL_Log("Slab %d free list size: %d\n", slab, this_slab_free);
    total_free_headers += this_slab_free;
  }

  SDL_Log("Total free headers: %d\n", total_free_headers);
}

static void SaveBitmap(SDL_Surface *surface) {
  char fname[32];
  snprintf(fname, sizeof(fname), "bpm_snapshot_%d.bmp", global_bitmap_index);
  global_bitmap_index += 1;
  SDL_SaveBMP(surface, fname);
  SDL_Log("Saved bitmap %s\n", fname);
}

static void SetActiveDevice(SharedMemoryContext *context, DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  if (device_id < pool->num_devices) {
    global_active_device = device_id;
    SDL_Log("Viewing Device %u\n", device_id);
  }
}

static void HandleInput(SharedMemoryContext *context, bool *running,
                        SDL_Surface *surface, char *shmem_name) {
  SDL_Event event;
  while (SDL_PollEvent(&event)) {
    switch (event.type) {
      case SDL_WINDOWEVENT: {
        switch (event.window.event) {
          case SDL_WINDOWEVENT_CLOSE: {
            *running = false;
            break;
          }
            // TODO(chogan):
            // case SDL_WINDOWEVENT_RESIZED: {
            //   int new_width = event.window.data1;
            //   int new_height = event.window.data2;
            //   ResizeWindow(new_width, new_height);
            //   break;
            // }
        }
        break;
      }
      case SDL_KEYUP: {
        switch (event.key.keysym.scancode) {
          case SDL_SCANCODE_0: {
            if (global_active_segment == ActiveSegment::BufferPool) {
              SetActiveDevice(context, 0);
            } else {
              global_active_heap = ActiveHeap::Id;
            }
            break;
          }
          case SDL_SCANCODE_1: {
            if (global_active_segment == ActiveSegment::BufferPool) {
              SetActiveDevice(context, 1);
            } else {
              global_active_heap = ActiveHeap::Map;
            }
            break;
          }
          case SDL_SCANCODE_2: {
            SetActiveDevice(context, 2);
            break;
          }
          case SDL_SCANCODE_3: {
            SetActiveDevice(context, 3);
            break;
          }
          case SDL_SCANCODE_4: {
            SetActiveDevice(context, 4);
            break;
          }
          case SDL_SCANCODE_5: {
            SetActiveDevice(context, 5);
            break;
          }
          case SDL_SCANCODE_6: {
            SetActiveDevice(context, 6);
            break;
          }
          case SDL_SCANCODE_7: {
            SetActiveDevice(context, 7);
            break;
          }
          case SDL_SCANCODE_B: {
            global_active_segment =  ActiveSegment::BufferPool;
            SDL_Log("Viewing BufferPool segment\n");
            break;
          }
          case SDL_SCANCODE_C: {
            PrintBufferCounts(context, global_active_device);
            break;
          }
          case SDL_SCANCODE_F: {
            PrintFreeListSizes(context, global_active_device);
            break;
          }
          case SDL_SCANCODE_M: {
            if (id_debug_state == 0 || map_debug_state == 0) {
              SDL_Log("Must enable HERMES_DEBUG_HEAP to inspect metadata\n");
            } else {
              global_active_segment =  ActiveSegment::Metadata;
              SDL_Log("Viewing Metadata segment\n");
            }
            break;
          }
          case SDL_SCANCODE_R: {
            ReloadSharedMemory(context, shmem_name);
            SDL_Log("Reloaded shared memory\n");
            break;
          }
          case SDL_SCANCODE_S: {
            SaveBitmap(surface);
            break;
          }
          case SDL_SCANCODE_ESCAPE: {
            *running = false;
            break;
          }
          default:
            break;
        }
        break;
      }
    }
  }
}

static void InitColors(SDL_PixelFormat *format) {
  global_colors[kColor_Red] = SDL_MapRGB(format, 255, 0, 0);
  global_colors[kColor_Yellow] = SDL_MapRGB(format, 255, 255, 0);
  global_colors[kColor_Green] = SDL_MapRGB(format, 0, 255, 0);
  global_colors[kColor_Cyan] = SDL_MapRGB(format, 24, 154, 211);
  global_colors[kColor_Grey] = SDL_MapRGB(format, 128, 128, 128);
  global_colors[kColor_White] = SDL_MapRGB(format, 255, 255, 255);
  global_colors[kColor_Magenta] = SDL_MapRGB(format, 255, 0, 255);
  global_colors[kColor_Black] = SDL_MapRGB(format, 0, 0, 0);
}

void DisplayBufferPoolSegment(SharedMemoryContext *context,
                              SDL_Surface *back_buffer, int width, int height) {
  int ending_y = 0;
  if (global_active_device == 0) {
    ending_y = DrawBufferPool(context, back_buffer, width, height,
                              global_active_device);
    DrawEndOfRamBuffers(context, back_buffer, width, height);
  } else {
    ending_y = DrawFileBuffers(context, back_buffer, width, height,
                               global_active_device);
  }

  ending_y += 2;
  SDL_Rect dividing_line = {0, ending_y, width, 1};
  SDL_FillRect(back_buffer, &dividing_line,
               global_colors[kColor_Magenta]);
  DrawHeaders(context, back_buffer, width, height,
              ending_y + 5, global_active_device);
}


Point AddrToPoint(HeapMetadata *hmd, uintptr_t addr) {
  Point result = {};
  f32 t = (f32)(addr - (uintptr_t)hmd->heap_base) / (f32)hmd->heap_size;
  u32 slot_number = (u32)(t * hmd->total_slots);
  result.x = slot_number % hmd->screen_width;
  result.y = (slot_number / hmd->screen_width) * 5 + hmd->y_offset;

  return result;
}

int GetAllocationWidth(HeapMetadata *hmd, size_t size) {
  f32 t = (f32)size / (f32)hmd->heap_size;
  int result = t * (f32)hmd->total_slots;

  return result;
}

void DrawAllocatedHeapBlocks(DebugState *state, HeapMetadata *hmd, Heap *heap,
                             SDL_Surface *surface) {
  global_color_counter = 0;
  for (u32 i = 0; i < state->allocation_count; ++i) {
    DebugHeapAllocation *allocation = &state->allocations[i];
    u8 *heap_ptr = HeapOffsetToPtr(heap, allocation->offset);
    uintptr_t ptr_adjustment = heap->grows_up ? 0 : allocation->size;
    Point p = AddrToPoint(hmd, (uintptr_t)heap_ptr - ptr_adjustment);
    int pixel_width = GetAllocationWidth(hmd, allocation->size);
    SDL_Rect rect = {p.x, p.y, pixel_width, hmd->h};

    global_color_counter++;
    if (global_color_counter == kColor_HeapMax) {
      global_color_counter = 0;
    }
    u32 color = global_colors[global_color_counter];
    DrawWrappingRect(&rect, hmd->screen_width, 0, surface, color);
  }
}

void DrawHeapExtent(HeapMetadata *hmd, Heap *heap, int w,
                    SDL_Surface *surface) {
  u8 *extent = HeapExtentToPtr(heap);
  Point extent_point = AddrToPoint(hmd, (uintptr_t)extent);
  SDL_Rect rect = {extent_point.x, extent_point.y, w, hmd->h};
  DrawWrappingRect(&rect, hmd->screen_width, 0, surface,
                   global_colors[kColor_White]);
}

void DrawHeap(HeapMetadata *hmd, Heap *heap, int w, SDL_Surface *surface) {
  Point heap_xy = AddrToPoint(hmd, (uintptr_t)heap);
  SDL_Rect heap_rect = {heap_xy.x, heap_xy.y, w, hmd->h};
  DrawWrappingRect(&heap_rect, hmd->screen_width, 0, surface,
                   global_colors[kColor_White]);
}

int ClampWidthToExtent(HeapMetadata *hmd, Heap *heap, u8 **block_addr,
                       int block_width) {
  int result = block_width;
  u32 extent_offset_bytes = heap->extent;
  u32 addr_offset_bytes = GetHeapOffset(heap, *block_addr);
  u32 extent_slot = hmd->bytes_to_slots * (f32)extent_offset_bytes;
  u32 addr_slot = hmd->bytes_to_slots * (f32)addr_offset_bytes;

  if (heap->grows_up) {
    if (addr_slot + block_width > extent_slot) {
      result = extent_slot - addr_slot;
    }
  } else {
    result -= extent_slot - addr_slot;
    u8 *extent = HeapExtentToPtr(heap);
    *block_addr = extent;
  }

  return result;
}

void DrawFreeHeapBlocks(HeapMetadata *hmd, Heap *heap, SDL_Surface *surface) {
  FreeBlock *head = GetHeapFreeList(heap);
  while (head) {
    uintptr_t ptr_adjustment = heap->grows_up ? 0 : head->size;
    u8 *addr = (u8 *)head - ptr_adjustment;
    int pixel_width = GetAllocationWidth(hmd, head->size);
    int clamped_width = ClampWidthToExtent(hmd, heap, &addr, pixel_width);
    Point p = AddrToPoint(hmd, (uintptr_t)addr);
    SDL_Rect rect = {p.x, p.y, clamped_width, hmd->h};
    DrawWrappingRect(&rect, hmd->screen_width, 0, surface,
                     global_colors[kColor_White]);
    head = NextFreeBlock(heap, head);
  }
}

void DisplayMetadataSegment(SharedMemoryContext *context,
                            SDL_Surface *surface, int width, int height,
                            DebugState *map_debug_state,
                            DebugState *id_debug_state) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  int pad = 2;
  int w = 5;
  int h = 5;
  int x = 0;
  int y = 0;

  // BucketInfo
  for (size_t i = 0; i < mdm->max_buckets; ++i) {
    BucketInfo *info = LocalGetBucketInfoByIndex(mdm, i);

    if (x > width) {
      x = 0;
      y += h + pad;
    }

    if (y + h + pad > height) {
      SDL_Log("Not enough room to display all Buckets\n");
      break;
    }

    u32 rgb_color = info->active ? global_colors[kColor_White] :
      global_colors[kColor_Red];

    SDL_Rect rect = {x, y, w, h};
    DrawWrappingRect(&rect, width, pad, surface, rgb_color);
    x += w;
  }

  // VBucketInfo
  for (size_t i = 0; i < mdm->max_vbuckets; ++i) {
    VBucketInfo *info = GetVBucketInfoByIndex(mdm, i);

    if (x > width) {
      x = 0;
      y += h + pad;
    }

    if (y + h + pad > height) {
      SDL_Log("Not enough room to display all Buckets\n");
      break;
    }

    u32 rgb_color = info->active ? global_colors[kColor_White] :
      global_colors[kColor_Yellow];
    SDL_Rect rect = {x, y, w, h};
    DrawWrappingRect(&rect, width, pad, surface, rgb_color);
    x += w;
  }

  int ending_y = y + h + pad;
  SDL_Rect dividing_line = {0, ending_y, width, 1};
  SDL_FillRect(surface, &dividing_line, global_colors[kColor_Magenta]);

  ending_y += h + pad;

  // Draw Metadata Heap

  x = 0;
  y = ending_y;
  height -= ending_y;

  Heap *id_heap = GetIdHeap(mdm);
  Heap *map_heap = GetMapHeap(mdm);

  int num_pixel_rows = height - 1 - ending_y;
  num_pixel_rows = RoundDownToMultiple(num_pixel_rows, h);

  HeapMetadata hmd = {};
  hmd.num_rows = num_pixel_rows / h;
  hmd.total_slots = width * hmd.num_rows;
  hmd.heap_base = (u8 *)map_heap;
  hmd.heap_size = (u8 *)(id_heap) - (u8 *)(map_heap + 1);
  hmd.screen_width = width;
  hmd.y_offset = ending_y;
  hmd.h = h;
  hmd.slots_to_bytes = (f32)hmd.heap_size / (f32)hmd.total_slots;
  hmd.bytes_to_slots = 1.0f / hmd.slots_to_bytes;
  assert(hmd.heap_size > hmd.total_slots);

  switch (global_active_heap) {
    case ActiveHeap::Map: {
      DrawAllocatedHeapBlocks(map_debug_state, &hmd, map_heap, surface);
      DrawFreeHeapBlocks(&hmd, map_heap, surface);
      break;
    }
    case ActiveHeap::Id: {
      DrawAllocatedHeapBlocks(id_debug_state, &hmd, id_heap, surface);
      // DrawFreeHeapBlocks(&hmd, id_heap, surface);
      break;
    }
  }

  DrawHeapExtent(&hmd, map_heap, w, surface);
  DrawHeapExtent(&hmd, id_heap, w, surface);

  DrawHeap(&hmd, map_heap, w, surface);
  DrawHeap(&hmd, id_heap, w, surface);
}

int main() {
  SDL_Init(SDL_INIT_VIDEO);

  int window_width = 1200;
  int window_height = 600;

  SDL_Window *window = CreateWindow(window_width, window_height);
  SDL_Surface *screen = GetScreen(window);
  SDL_Surface *back_buffer = CreateBackBuffer(window_width, window_height);

  InitColors(back_buffer->format);

  char full_shmem_name[kMaxBufferPoolShmemNameLength];
  char base_shmem_name[] = "/hermes_buffer_pool_";
  MakeFullShmemName(full_shmem_name, base_shmem_name);
  SharedMemoryContext context = GetSharedMemoryContext(full_shmem_name);
  if (context.shm_base == 0) {
    SDL_Log("Couldn't open BufferPool shared memory\n");
    exit(1);
  }

  SharedMemoryContext id_debug_context =
    GetSharedMemoryContext(global_debug_id_name);
  SharedMemoryContext map_debug_context =
    GetSharedMemoryContext(global_debug_map_name);

  if (id_debug_context.shm_base) {
    id_debug_state = (DebugState *)id_debug_context.shm_base;
  }
  if (map_debug_context.shm_base) {
    map_debug_state = (DebugState *)map_debug_context.shm_base;
  }

  bool running = true;
  while (running) {
    HandleInput(&context, &running, screen, full_shmem_name);

    SDL_FillRect(back_buffer, NULL, global_colors[kColor_Black]);

    switch (global_active_segment) {
      case ActiveSegment::BufferPool: {
        DisplayBufferPoolSegment(&context, back_buffer, window_width,
                                 window_height);
        break;
      }
      case ActiveSegment::Metadata: {
        DisplayMetadataSegment(&context, back_buffer, window_width,
                               window_height, map_debug_state, id_debug_state);
        break;
      }
    }

    SDL_BlitSurface(back_buffer, NULL, screen, NULL);
    SDL_UpdateWindowSurface(window);

    SDL_Delay(60);
  }

  ReleaseSharedMemoryContext(&context);

  UnmapSharedMemory(&id_debug_context);
  UnmapSharedMemory(&map_debug_context);

  SDL_FreeSurface(back_buffer);
  SDL_DestroyWindow(window);
  SDL_Quit();

  return 0;
}
