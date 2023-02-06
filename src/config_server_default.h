#ifndef HERMES_SRC_CONFIG_SERVER_DEFAULT_H_
#define HERMES_SRC_CONFIG_SERVER_DEFAULT_H_
const char* kServerDefaultConfigStr = 
"# Example Hermes configuration file\n"
"\n"
"### Define properties of the storage devices\n"
"devices:\n"
"  # The name of the device.\n"
"  # It can be whatever the user wants, there are no special names\n"
"  ram:\n"
"    # The mount point of each device. RAM should be the empty string. For block\n"
"    # devices, this is the directory where Hermes will create buffering files. For\n"
"    # object storage or cloud targets, this will be a url.\n"
"    mount_point: \"\"\n"
"\n"
"    # The maximum buffering capacity in MiB of each device. Here we say that all 4\n"
"    # devices get 50 MiB of buffering capacity.\n"
"    capacity: 5000MB\n"
"\n"
"    # The size of the smallest available buffer in KiB. In general this should be\n"
"    # the page size of your system for byte addressable storage, and the block size\n"
"    # of the storage device for block addressable storage.\n"
"    block_size: 4KB\n"
"\n"
"    # The number of blocks (the size of which is chosen in block_sizes_kb) that each\n"
"    # device should contain for each slab (controlled by num_slabs). This allows for\n"
"    # precise control of the distibution of buffer sizes.\n"
"    slab_units: [1, 4, 16, 32]\n"
"\n"
"    # The maximum theoretical bandwidth (as advertised by the manufacturer) in\n"
"    # Possible units: KBps, MBps, GBps\n"
"    bandwidth: 6000MBps\n"
"\n"
"    # The latency of each device (as advertised by the manufacturer).\n"
"    # Possible units: ns, us, ms, s\n"
"    latency: 15us\n"
"\n"
"    # For each device, indicate \'1\' if it is shared among nodes (e.g., burst\n"
"    # buffers), or \'0\' if it is per node (e.g., local NVMe).\n"
"    is_shared_device: false\n"
"\n"
"    # For each device, the minimum and maximum percent capacity threshold at which\n"
"    # the BufferOrganizer will trigger. Decreasing the maximum thresholds will cause\n"
"    # the BufferOrganizer to move data to lower devices, making more room in faster\n"
"    # devices (ideal for write-heavy workloads). Conversely, increasing the minimum\n"
"    # threshold will cause data to be moved from slower devices into faster devices\n"
"    # (ideal for read-heavy workloads). For example, a maximum capacity threshold of\n"
"    # 0.8 would have the effect of always keeping 20% of the device\'s space free for\n"
"    # incoming writes. Conversely, a minimum capacity threshold of 0.3 would ensure\n"
"    # that the device is always at least 30% occupied.\n"
"    borg_capacity_thresh: [0.0, 1.0]\n"
"\n"
"  nvme:\n"
"    mount_point: \"./\"\n"
"    capacity: 50MB\n"
"    block_size: 4KB\n"
"    slab_units: [ 1, 4, 16, 32 ]\n"
"    bandwidth: 1GBps\n"
"    latency: 600us\n"
"    is_shared_device: false\n"
"    borg_capacity_thresh: [ 0.0, 1.0 ]\n"
"\n"
"  ssd:\n"
"    mount_point: \"./\"\n"
"    capacity: 50MB\n"
"    block_size: 4KB\n"
"    slab_units: [ 1, 4, 16, 32 ]\n"
"    bandwidth: 500MBps\n"
"    latency: 1200us\n"
"    is_shared_device: false\n"
"    borg_capacity_thresh: [ 0.0, 1.0 ]\n"
"\n"
"  pfs:\n"
"    mount_point: \"./\"\n"
"    capacity: inf\n"
"    block_size: 64KB # The stripe size of PFS\n"
"    slab_units: [ 1, 4, 16, 32 ]\n"
"    bandwidth: 100MBps # Per-device bandwidth\n"
"    latency: 200ms\n"
"    is_shared_device: true\n"
"    borg_capacity_thresh: [ 0.0, 1.0 ]\n"
"\n"
"### Define properties of RPCs\n"
"rpc:\n"
"  # A path to a file containing a list of server names, 1 per line. If your\n"
"  # servers are named according to a pattern (e.g., server-1, server-2, etc.),\n"
"  # prefer the `rpc_server_base_name` and `rpc_host_number_range` options. If this\n"
"  # option is not empty, it will override anything in `rpc_server_base_name`.\n"
"  host_file: \"\"\n"
"\n"
"  # Host names are constructed as \"base_name +\n"
"  # host_number + rpc_server_suffix.\" Each entry in the rpc_host_number_range_list\n"
"  # can be either a single number, or a range, which is 2 numbers separated by a\n"
"  # hyphen (-). For example the list {01, 03-05, 07, 08-10} will be expanded to\n"
"  # {01, 03, 04, 05, 07, 08, 09, 10}.\n"
"  base_name: \"localhost\"\n"
"  host_number_range: []\n"
"  suffix: \"\"\n"
"\n"
"  # The RPC protocol. This must come from the documentation of the specific RPC\n"
"  # library in use.\n"
"  protocol: \"ofi+sockets\"\n"
"\n"
"  # RPC domain name for verbs transport. Blank for tcp.\n"
"  domain: \"\"\n"
"\n"
"  # Desired RPC port number.\n"
"  port: 8080\n"
"\n"
"  # The number of handler threads for each RPC server.\n"
"  num_threads: 1\n"
"\n"
"### Define properties of the BORG\n"
"buffer_organizer:\n"
"  # The number of threads used in the background organization of internal Hermes buffers.\n"
"  num_threads: 4\n"
"\n"
"  # Desired RPC port number for buffer organizer.\n"
"  port: 8081\n"
"\n"
"### Define the default data placement policy\n"
"dpe:\n"
"  # Choose Random, RoundRobin, or MinimizeIoTime\n"
"  default_placement_policy: \"MinimizeIoTime\"\n"
"\n"
"  # If true (1) the RoundRobin placement policy algorithm will split each Blob\n"
"  # into a random number of smaller Blobs.\n"
"  default_rr_split: 0\n"
"\n"
"# The shared memory prefix for the hermes shared memory segment. A user name\n"
"# will be automatically appended.\n"
"shmem_name: \"/hermes_shm_\"\n"
"\n"
"# The interval in milliseconds at which to update the global system view.\n"
"system_view_state_update_interval_ms: 1000\n";
#endif  // HERMES_SRC_CONFIG_SERVER_DEFAULT_H_