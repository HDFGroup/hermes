# Buffer Pool Visualizer

A visual debugging tool for the Hermes `BufferPool` and `MetadataManager`.

To run, start a Hermes instance and then run `make viz` from the build
directory. It can also visualize a crashed Hermes run if the shared memory
segment still exists.

To enable in the build, set `HERMES_BUILD_BUFFER_POOL_VISUALIZER=ON` in CMake.
In order to visualize the `MetadataManager`, Hermes must be built with the CMake
option `HERMES_DEBUG_HEAP=ON`.

## Dependencies

[SDL](https://www.libsdl.org/) is the only additional dependency. On Ubuntu,
`sudo apt install libsdl2-dev`.

## Controls

| Key | Functionality                                          |
|-----|--------------------------------------------------------|
| 0-9 | If viewing the `BufferPool`, display Tier N            |
| 0   | If viewing the `MetadataManager`, display the ID heap  |
| 1   | If viewing the `MetadataManager`, display the map heap |
| b   | View the `BufferPool`                                  |
| c   | Print a count of all buffers and slabs                 |
| f   | Print the sizes of the buffer free lists               |
| m   | View the `MetadataManager`                             |
| r   | Reload the shared memory file                          |
| s   | Save a bitmap of the current view                      |
| ESC | Exit the visualizer                                    |

## Description of Visual Information

### `BufferPool` mode

![Buffer Pool Visualizer](https://github.com/HDFGroup/hermes/wiki/images/bp_viz.png)

### `MetadataManager` mode

![Metadata Visualizer](https://github.com/HDFGroup/hermes/wiki/images/mdm_viz.png)
