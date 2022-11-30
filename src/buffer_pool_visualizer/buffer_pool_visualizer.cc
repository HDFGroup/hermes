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

#include <unordered_map>

#include "SDL.h"

#include "hermes_types.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "metadata_management.h"
#include "metadata_management_internal.h"
#include "debug_state.h"
#include "utils.h"

#define DRAW_BUCKETS 0

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

enum IdHeapColors {
  kColor_Blue1,
  kColor_Blue2,
  kColor_Blue3,
  kColor_Blue4,
  kColor_Blue5,

  kIdHeapColors_Count
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
static u32 global_id_colors[kIdHeapColors_Count];
static int global_bitmap_index;
static DeviceID global_active_device;
static ActiveSegment global_active_segment = ActiveSegment::Metadata;
static ActiveHeap global_active_heap;
static u32 global_color_counter;
static DebugState *global_id_debug_state;
static DebugState *global_map_debug_state;

/**
  A structure to represent range from start to end.
*/
struct Range {
  int start;                    /**< start value of range */
  int end;                      /**< end value of range */
};

/**
 A structure to represent a point at (x,y) location
*/
struct Point {
  int x;                        /**< x location value */
  int y;                        /**< y location value */
};

/**
 A structure to represent heap for metadata
*/
struct HeapMetadata {
  u8 *heap_base;                /**< pointer to heap */
  ptrdiff_t heap_size;          /**< size of heap */
  int total_slots;              /**< total number of slots */
  int screen_width;             /**< screen width */
  int num_rows;                 /**< number of rows */
  int y_offset;                 /**< y offset */
  int h;                        /**< height */
  f32 slots_to_bytes;           /**< slots:bytes ratio */
  f32 bytes_to_slots;           /**< bytes:slots ratio */
};

/**
 A structure to represent window
*/
struct WindowData {
  SDL_Window *window;           /**< pointer to window */
  SDL_Surface *back_buffer;     /**< buffer */
  SDL_Surface *screen;          /**< screen */
  int width;                    /**< width */
  int height;                   /**< height */
  bool running;                 /**< is window running? */
};

static SDL_Window *CreateWindow(int width, int height) {
  SDL_Window *result = SDL_CreateWindow("BufferPool Visualizer",
                                        1920 - width,
                                        0,
                                        width,
                                        height,
                                        SDL_WINDOW_RESIZABLE);

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
      std::string index_str = std::to_string(index) + ":" + std::to_string(j);
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

static void HandleInput(SharedMemoryContext *context, WindowData *win_data,
                        char *shmem_name) {
  SDL_Surface *surface = win_data->screen;

  SDL_Event event;
  while (SDL_PollEvent(&event)) {
    switch (event.type) {
      case SDL_WINDOWEVENT: {
        switch (event.window.event) {
          case SDL_WINDOWEVENT_CLOSE: {
            win_data->running = false;
            break;
          }
          case SDL_WINDOWEVENT_RESIZED: {
            win_data->width = event.window.data1;
            win_data->height = event.window.data2;
            SDL_FreeSurface(win_data->back_buffer);
            win_data->back_buffer = CreateBackBuffer(win_data->width,
                                                     win_data->height);
            win_data->screen = GetScreen(win_data->window);
            break;
          }
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
            if (global_id_debug_state == 0 || global_map_debug_state == 0) {
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
            win_data->running = false;
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

  global_id_colors[kColor_Blue1] = SDL_MapRGB(format, 0x03, 0x04, 0x5e);
  global_id_colors[kColor_Blue2] = SDL_MapRGB(format, 0, 0x77, 0xb6);
  global_id_colors[kColor_Blue3] = SDL_MapRGB(format, 0, 0xb4, 0xd8);
  global_id_colors[kColor_Blue4] = SDL_MapRGB(format, 0x90, 0xe0, 0xef);
  global_id_colors[kColor_Blue5] = SDL_MapRGB(format, 0xca, 0xf0, 0xf8);
}

static void DisplayBufferPoolSegment(SharedMemoryContext *context,
                                     WindowData *win_data) {
  SDL_Surface *back_buffer = win_data->back_buffer;
  int width = win_data->width;
  int height = win_data->height;
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

static inline int SlotsToPixelWidth(HeapMetadata *hmd, f32 size) {
  int result = (int)(size * hmd->bytes_to_slots);

  return result;
}

static SDL_Rect OffsetToRect(HeapMetadata *hmd, Heap *heap, u32 offset,
                             u32 size, bool free_block) {
  if (!heap->grows_up) {
    assert(hmd->heap_size >= offset);
    // Make offset relative to start of heap
    offset = hmd->heap_size - offset;

    u32 extent_offset_from_start = hmd->heap_size - heap->extent;
    if (offset <= extent_offset_from_start && free_block) {
      offset = extent_offset_from_start;
      size = hmd->slots_to_bytes * 3;
    }
  } else {
    assert(offset <= heap->extent);
    if ((offset + size) > heap->extent && free_block) {
      size = heap->extent - offset;
    }
  }

  u32 slot_number = (u32)(hmd->bytes_to_slots * (f32)offset);
  int x =  slot_number % hmd->screen_width;
  int y = (slot_number / hmd->screen_width * hmd->h + hmd->y_offset);
  int w = SlotsToPixelWidth(hmd, size);
  int h = hmd->h;

  SDL_Rect result = {x, y, w, h};

  return result;
}

static void DrawAllocatedHeapBlocks(DebugState *state, HeapMetadata *hmd,
                                    Heap *heap, SDL_Surface *surface) {
  global_color_counter = 0;
  state->mutex.Lock();;
  for (u32 i = 0; i < state->allocation_count; ++i) {
    DebugHeapAllocation *allocation = &state->allocations[i];
    SDL_Rect rect = OffsetToRect(hmd, heap, allocation->offset,
                                 allocation->size, false);

    global_color_counter++;
    u32 color_max = kIdHeapColors_Count;
    // int color_max = kColor_HeapMax;
    if (global_color_counter == color_max) {
      global_color_counter = 0;
    }
    // u32 color = global_colors[global_color_counter];
    u32 color = global_id_colors[global_color_counter];
    DrawWrappingRect(&rect, hmd->screen_width, 0, surface, color);
  }
  state->mutex.Unlock();;
}

static void DrawHeapExtent(HeapMetadata *hmd, Heap *heap, int w,
                    SDL_Surface *surface) {
  u32 offset = heap->extent;
  if (!heap->grows_up) {
    assert(hmd->heap_size >= offset);
    // Make offset relative to start of heap
    offset = hmd->heap_size - offset;
  }

  u32 slot_number = (u32)(hmd->bytes_to_slots * (f32)offset);
  int x =  slot_number % hmd->screen_width;
  int y = (slot_number / hmd->screen_width * hmd->h + hmd->y_offset);
  SDL_Rect rect = {x, y, w, hmd->h};
  DrawWrappingRect(&rect, hmd->screen_width, 0, surface,
                   global_colors[kColor_Magenta]);
}

static void DrawFreeHeapBlocks(HeapMetadata *hmd, Heap *heap,
                               SDL_Surface *surface) {
  heap->mutex.Lock();
  u32 offset = heap->free_list_offset;
  FreeBlock *head = GetHeapFreeList(heap);
  while (head) {
    if (!heap->grows_up) {
      offset = offset - sizeof(FreeBlock) + head->size;
    }
    SDL_Rect rect = OffsetToRect(hmd, heap, offset, head->size, true);
    DrawWrappingRect(&rect, hmd->screen_width, 0, surface,
                     global_colors[kColor_White]);
    offset = head->next_offset;
    head = NextFreeBlock(heap, head);
  }
  heap->mutex.Unlock();
}

#if DRAW_BUCKETS
static int DrawBucketsAndVBuckets(MetadataManager *mdm, SDL_Surface *surface,
                           int width, int height, int w, int h, int pad) {
  int x = 0;
  int y = 0;

  // BucketInfo
  for (size_t i = 0; i < mdm->max_buckets; ++i) {
    mdm->bucket_mutex.Lock();
    BucketInfo *info = LocalGetBucketInfoByIndex(mdm, i);
    bool active = info->active;
    mdm->bucket_mutex.Unlock();

    if (x > width) {
      x = 0;
      y += h + pad;
    }

    if (y + h + pad > height) {
      SDL_Log("Not enough room to display all Buckets\n");
      break;
    }

    u32 rgb_color = active ? global_colors[kColor_White] :
      global_colors[kColor_Red];

    SDL_Rect rect = {x, y, w, h};
    DrawWrappingRect(&rect, width, pad, surface, rgb_color);
    x += w;
  }

  // VBucketInfo
  for (size_t i = 0; i < mdm->max_vbuckets; ++i) {
    mdm->vbucket_mutex.Lock();
    VBucketInfo *info = GetVBucketInfoByIndex(mdm, i);
    bool active = info->active;
    mdm->vbucket_mutex.Unlock();

    if (x > width) {
      x = 0;
      y += h + pad;
    }

    if (y + h + pad > height) {
      SDL_Log("Not enough room to display all Buckets\n");
      break;
    }

    u32 rgb_color = active ? global_colors[kColor_White] :
      global_colors[kColor_Yellow];
    SDL_Rect rect = {x, y, w, h};
    DrawWrappingRect(&rect, width, pad, surface, rgb_color);
    x += w;
  }

  int ending_y = y + h + pad;
  SDL_Rect dividing_line = {0, ending_y, width, 1};
  SDL_FillRect(surface, &dividing_line, global_colors[kColor_Magenta]);

  ending_y += h + pad;

  return ending_y;
}
#endif  // DRAW_BUCKETS

static bool PopulateByteCounts(DebugState *state, Heap *heap,
                               std::vector<u8> &byte_counts, u32 heap_size) {
  for (size_t i = 0; i < state->allocation_count; ++i) {
    DebugHeapAllocation *dha = &state->allocations[i];
    u32 offset = dha->offset;

    if (!heap->grows_up) {
      assert(heap_size > dha->offset);
      offset = heap_size - dha->offset;
    }

    for (size_t j = offset; j < offset + dha->size; ++j) {
      byte_counts[j]++;
      // Ensure no 2 blocks refer to the same byte
      if (byte_counts[j] > 1) {
        SDL_Log("Byte %lu is referred to in %u blocks\n", j, byte_counts[j]);
        return false;
      }
    }
  }

  return true;
}

static bool CheckFreeBlocks(Heap *heap, const std::vector<u8> &byte_counts,
                            u32 heap_size) {
  heap->mutex.Lock();
  u32 extent_offset_from_start = heap->extent;
  if (!heap->grows_up) {
    extent_offset_from_start = heap_size - heap->extent;
  }

  bool result = true;
  u32 offset = heap->free_list_offset;
  FreeBlock *head = GetHeapFreeList(heap);
  bool stop = false;
  while (head && !stop) {
    if (!heap->grows_up) {
      offset = offset - sizeof(FreeBlock) + head->size;
      offset = heap_size - offset;
    }

    size_t start = 0;
    size_t stop = 0;

    if (heap->grows_up) {
      start = offset;
      stop = std::min(offset + head->size, extent_offset_from_start);
    } else {
      start = std::max(offset, extent_offset_from_start);
      stop = offset + head->size;
    }

    for (size_t i = start; i < stop; ++i) {
      if (byte_counts[i] != 0) {
        SDL_Log("Byte %lu is referred to by a free block with offset %u\n", i,
                offset);
        stop = true;
        result = false;
        break;
      }
    }

    offset = head->next_offset;
    head = NextFreeBlock(heap, head);
  }
  heap->mutex.Unlock();

  return result;
}

static bool CheckOverlap(HeapMetadata *hmd, Heap *map_heap,
                         Heap *id_heap) {
  std::vector<u8> heap_byte_counts(hmd->heap_size, 0);

  global_id_debug_state->mutex.Lock();
  global_map_debug_state->mutex.Lock();

  bool result = true;
  if (!PopulateByteCounts(global_id_debug_state, id_heap, heap_byte_counts,
                          hmd->heap_size)) {
    result = false;
  }
  if (!PopulateByteCounts(global_map_debug_state, map_heap, heap_byte_counts,
                          hmd->heap_size)) {
    result = false;
  }

  // Make sure free blocks don't overlap with any allocated blocks
  if (!CheckFreeBlocks(map_heap, heap_byte_counts, hmd->heap_size)) {
    result = false;
  }
  if (!CheckFreeBlocks(id_heap, heap_byte_counts, hmd->heap_size)) {
    result = false;
  }

  global_id_debug_state->mutex.Unlock();
  global_map_debug_state->mutex.Unlock();

  return result;
}

static void DisplayMetadataSegment(SharedMemoryContext *context,
                                   WindowData *win_data,
                                   DebugState *map_debug_state,
                                   DebugState *id_debug_state) {
  SDL_Surface *surface = win_data->back_buffer;
  int width = win_data->width;
  int height = win_data->height;

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  int pad = 2;
  int w = 3;
  int h = 3;

#if DRAW_BUCKETS
  int ending_y = DrawBucketsAndVBuckets(mdm, surface, width, height, w, h);
# else
  int ending_y = pad;
#endif

  int num_pixel_rows = height - 1 - ending_y;
  num_pixel_rows = RoundDownToMultiple(num_pixel_rows, h);

  Heap *id_heap = GetIdHeap(mdm);
  Heap *map_heap = GetMapHeap(mdm);

  HeapMetadata hmd = {};
  hmd.num_rows = (num_pixel_rows / h) - 1;
  hmd.total_slots = width * hmd.num_rows;
  hmd.heap_base = (u8 *)(map_heap + 1);
  hmd.heap_size = (u8 *)(id_heap) - (u8 *)(map_heap + 1);
  hmd.screen_width = width;
  hmd.y_offset = ending_y;
  hmd.h = h;
  hmd.slots_to_bytes = (f32)hmd.heap_size / (f32)hmd.total_slots;
  hmd.bytes_to_slots = 1.0f / hmd.slots_to_bytes;
  assert(hmd.heap_size > hmd.total_slots);

  // Map Heap
  DrawAllocatedHeapBlocks(map_debug_state, &hmd, map_heap, surface);
  DrawFreeHeapBlocks(&hmd, map_heap, surface);
  // ID Heap
  DrawFreeHeapBlocks(&hmd, id_heap, surface);
  DrawAllocatedHeapBlocks(id_debug_state, &hmd, id_heap, surface);

  DrawHeapExtent(&hmd, map_heap, w, surface);
  DrawHeapExtent(&hmd, id_heap, w, surface);

  if (!CheckOverlap(&hmd, map_heap, id_heap)) {
    win_data->running = false;
  }
}

int main() {
  SDL_Init(SDL_INIT_VIDEO);

  WindowData win_data = {};
  win_data.width = 1200;
  win_data.height = 600;
  win_data.window = CreateWindow(win_data.width, win_data.height);
  win_data.back_buffer = CreateBackBuffer(win_data.width, win_data.height);
  win_data.screen = GetScreen(win_data.window);
  win_data.running = true;

  InitColors(win_data.back_buffer->format);

  char full_shmem_name[kMaxBufferPoolShmemNameLength];
  char base_shmem_name[] = "/hermes_buffer_pool_";
  MakeFullShmemName(full_shmem_name, base_shmem_name);
  SharedMemoryContext context = GetSharedMemoryContext(full_shmem_name);
  if (context.shm_base == 0) {
    SDL_Log("Couldn't open BufferPool shared memory\n");
    exit(1);
  }

  while (win_data.running) {
    HandleInput(&context, &win_data, full_shmem_name);

    SDL_FillRect(win_data.back_buffer, NULL, global_colors[kColor_Black]);

    switch (global_active_segment) {
      case ActiveSegment::BufferPool: {
        DisplayBufferPoolSegment(&context, &win_data);
        break;
      }
      case ActiveSegment::Metadata: {
        DisplayMetadataSegment(&context, &win_data, global_map_debug_state,
                               global_id_debug_state);
        break;
      }
    }

    SDL_BlitSurface(win_data.back_buffer, NULL, win_data.screen, NULL);
    SDL_UpdateWindowSurface(win_data.window);

    SDL_Delay(60);
  }

  ReleaseSharedMemoryContext(&context);

  SDL_FreeSurface(win_data.back_buffer);
  SDL_DestroyWindow(win_data.window);
  SDL_Quit();

  return 0;
}
