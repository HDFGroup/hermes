#include <unordered_map>

#include "SDL.h"

#include "hermes.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"

using namespace hermes;

enum class Color {
  kRed,
  kYellow,
  kGreen,
  kCyan,
  kGrey,
  kWhite,
  kMagenta,
  kBlack,

  kCount
};

static u32 global_colors[(int)Color::kCount];
static int global_bitmap_index;
static TierID global_active_tier;

struct Range {
  int start;
  int end;
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

// NOTE(chogan): This won't work if we allow non-ram headers to be dormant
// because getting the next dormant header can swap headers out of their preset
// index range
static Range GetHeaderIndexRange(SharedMemoryContext *context, TierID tier_id) {
  Range result = {};
  BufferPool *pool = GetBufferPoolFromContext(context);

  for (int i = 0; i < tier_id; ++i) {
    result.start += pool->num_headers[i];
  }
  result.end = result.start + pool->num_headers[tier_id];

  return result;
}

static int DrawBufferPool(SharedMemoryContext *context,
                          SDL_Surface *surface, int window_width,
                          int window_height, TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  size_t block_size = pool->block_sizes[tier_id];
  int block_width_pixels = 5;
  f32 memory_offset_to_pixels = (f32)(block_width_pixels) / (f32)(block_size);
  int pad = 2;
  int h = 5;
  int x = 0;
  int y = 0;
  int w = 0;
  int final_y = 0;

  std::unordered_map<std::string, int> block_refs;

  Range header_range = GetHeaderIndexRange(context, tier_id);

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
    for (int j = 0; j < pool->num_slabs[tier_id]; ++j) {
      if (num_blocks == GetSlabUnitSize(context, tier_id, j)) {
        color_index = j;
        break;
      }
    }

    assert(color_index >= 0);
    u32 rgb_color = global_colors[color_index];

    if (header->in_use) {
      // TODO(chogan): Just draw used in white, not whole capacity
      rgb_color = global_colors[(int)Color::kWhite];
    }

    x = pixel_offset % window_width;
    w = num_blocks * block_width_pixels;

    if (x + w >= window_width) {
      // NOTE(chogan): Draw the portion of the rect that fits on this line
      int this_line_width = window_width - (x + pad);
      SDL_Rect this_line_rect = {x + pad, y + pad, this_line_width, h - pad};
      SDL_FillRect(surface, &this_line_rect, rgb_color);

      // NOTE(chogan): Draw the rest of the rect on the next line.
      int off_screen_width = (x + w) - window_width;
      SDL_Rect next_line_rect = {0, y + pad + h, off_screen_width, h - pad};
      SDL_FillRect(surface, &next_line_rect, rgb_color);
    } else {
      // NOTE(chogan): Whole rect fits on one line
      SDL_Rect rect = {x + pad, y + pad, w - pad, h - pad};
      SDL_FillRect(surface, &rect, rgb_color);
    }

    if (y + pad >= final_y) {
      final_y = y + pad;
    }
  }

  // NOTE(chogan): Ensure that we are not drawing any portion of a buffer in
  // more than one place (i.e, no headers point to overlapping buffers)
  for (auto iter = block_refs.begin(); iter != block_refs.end(); ++iter) {
    if (tier_id == 0) {
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
                           TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  size_t block_size = pool->block_sizes[tier_id];
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
  std::vector<BlockMap> block_refs(pool->num_slabs[tier_id]);
  int block_refs_index = 0;

  Range header_range = GetHeaderIndexRange(context, tier_id);

  for (int i = header_range.start; i < header_range.end; ++i) {
    BufferHeader *header = headers + i;
    assert((u8 *)header < (u8 *)pool);
    if (HeaderIsDormant(header)) {
      continue;
    }

    int pixel_offset = (int)(header->data_offset * memory_offset_to_pixels);

    if (pixel_offset == 0) {
      if (y) {
        starting_y_offset = final_y + h;
        block_refs_index++;
      }
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
    for (int j = 0; j < pool->num_slabs[tier_id]; ++j) {
      if (num_blocks == GetSlabUnitSize(context, tier_id, j)) {
        color_index = j;
        break;
      }
    }

    assert(color_index >= 0);
    u32 rgb_color = global_colors[color_index];

    if (header->in_use) {
      // TODO(chogan): Just draw used in white, not whole capacity
      rgb_color = global_colors[(int)Color::kWhite];
    }

    x = pixel_offset % window_width;
    w = num_blocks * block_width_pixels;

    if (x + w >= window_width) {
      // NOTE(chogan): Draw the portion of the rect that fits on this line
      int this_line_width = window_width - (x + pad);
      SDL_Rect this_line_rect = {x + pad, y + pad, this_line_width, h - pad};
      SDL_FillRect(surface, &this_line_rect, rgb_color);

      // NOTE(chogan): Draw the rest of the rect on the next line.
      int off_screen_width = (x + w) - window_width;
      SDL_Rect next_line_rect = {0, y + pad + h, off_screen_width, h - pad};
      SDL_FillRect(surface, &next_line_rect, rgb_color);
    } else {
      // NOTE(chogan): Whole rect fits on one line
      SDL_Rect rect = {x + pad, y + pad, w - pad, h - pad};
      SDL_FillRect(surface, &rect, rgb_color);
    }

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

  int pixel_offset = (int)(pool->header_storage_offset *
                           memory_offset_to_pixels);
  int x = pixel_offset % window_width;
  int y = (pixel_offset / window_width) * h;

  if (y + pad < window_height) {
    SDL_Rect rect = {x + pad, y + pad, w - pad, h - pad};
    u32 color = global_colors[(int)Color::kMagenta];
    SDL_FillRect(surface, &rect, color);
  }
}

static void DrawHeaders(SharedMemoryContext *context, SDL_Surface *surface,
                        int window_width, int window_height, int starting_y,
                        TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  int pad = 1;
  int x = 0;
  int w = 2;
  int h = 5;
  int y = starting_y;


  Range index_range = GetHeaderIndexRange(context, tier_id);

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
      color = global_colors[(int)Color::kGrey];
    } else {
      color = global_colors[(int)Color::kGreen];
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

static void PrintBufferCounts(SharedMemoryContext *context, TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  std::vector<int> buffer_counts(pool->num_slabs[tier_id], 0);

  Range index_range = GetHeaderIndexRange(context, tier_id);

  for (int header_index = index_range.start;
       header_index < index_range.end;
       ++header_index) {
    BufferHeader *header = headers + header_index;
    if (HeaderIsDormant(header)) {
      continue;
    }

    for (size_t i = 0; i < buffer_counts.size(); ++i) {
      if (header->capacity == (u32)GetSlabBufferSize(context, tier_id, i)) {
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

static void PrintFreeListSizes(SharedMemoryContext *context, TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  int total_free_headers = 0;

  for (int slab = 0; slab < pool->num_slabs[tier_id]; ++slab) {
    BufferID next_free = PeekFirstFreeBufferId(context, tier_id, slab);
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

static void SetActiveTier(SharedMemoryContext *context, TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  if (tier_id < pool->num_tiers) {
    global_active_tier = tier_id;
    SDL_Log("Viewing Tier %u\n", tier_id);
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
            SetActiveTier(context, 0);
            break;
          }
          case SDL_SCANCODE_1: {
            SetActiveTier(context, 1);
            break;
          }
          case SDL_SCANCODE_2: {
            SetActiveTier(context, 2);
            break;
          }
          case SDL_SCANCODE_3: {
            SetActiveTier(context, 3);
            break;
          }
          case SDL_SCANCODE_4: {
            SetActiveTier(context, 4);
            break;
          }
          case SDL_SCANCODE_5: {
            SetActiveTier(context, 5);
            break;
          }
          case SDL_SCANCODE_6: {
            SetActiveTier(context, 6);
            break;
          }
          case SDL_SCANCODE_7: {
            SetActiveTier(context, 7);
            break;
          }
          case SDL_SCANCODE_C: {
            PrintBufferCounts(context, global_active_tier);
            break;
          }
          case SDL_SCANCODE_F: {
            PrintFreeListSizes(context, global_active_tier);
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
  global_colors[(int)Color::kRed] = SDL_MapRGB(format, 255, 0, 0);
  global_colors[(int)Color::kYellow] = SDL_MapRGB(format, 255, 255, 0);
  global_colors[(int)Color::kGreen] = SDL_MapRGB(format, 0, 255, 0);
  global_colors[(int)Color::kCyan] = SDL_MapRGB(format, 24, 154, 211);
  global_colors[(int)Color::kGrey] = SDL_MapRGB(format, 128, 128, 128);
  global_colors[(int)Color::kWhite] = SDL_MapRGB(format, 255, 255, 255);
  global_colors[(int)Color::kMagenta] = SDL_MapRGB(format, 255, 0, 255);
  global_colors[(int)Color::kBlack] = SDL_MapRGB(format, 0, 0, 0);
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
  SharedMemoryContext bp_context = GetSharedMemoryContext(full_shmem_name);
  if (bp_context.shm_base == 0) {
    SDL_Log("Couldn't open BufferPool shared memory\n");
    exit(1);
  }

  bool running = true;
  while (running) {
    HandleInput(&bp_context, &running, screen, full_shmem_name);

    SDL_FillRect(back_buffer, NULL, global_colors[(int)Color::kBlack]);

    int ending_y = 0;
    if (global_active_tier == 0) {
      ending_y = DrawBufferPool(&bp_context, back_buffer, window_width,
                                window_height, global_active_tier);
      DrawEndOfRamBuffers(&bp_context, back_buffer, window_width,
                          window_height);
    } else {
      ending_y = DrawFileBuffers(&bp_context, back_buffer, window_width,
                                 window_height, global_active_tier);
    }

    ending_y += 2;
    SDL_Rect dividing_line = {0, ending_y, window_width, 1};
    SDL_FillRect(back_buffer, &dividing_line,
                 global_colors[(int)Color::kMagenta]);
    DrawHeaders(&bp_context, back_buffer, window_width, window_height,
                ending_y + 5, global_active_tier);
    SDL_BlitSurface(back_buffer, NULL, screen, NULL);
    SDL_UpdateWindowSurface(window);

    SDL_Delay(60);
  }

  ReleaseSharedMemoryContext(&bp_context);

  SDL_FreeSurface(back_buffer);
  SDL_DestroyWindow(window);
  SDL_Quit();

  return 0;
}
