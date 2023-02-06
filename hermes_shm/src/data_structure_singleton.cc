#include <hermes_shm/constants/macros.h>
#include <hermes_shm/util/singleton.h>
#include <hermes_shm/thread/lock/mutex.h>
#include <hermes_shm/constants/data_structure_singleton_macros.h>

#include <hermes_shm/introspect/system_info.h>
template<> hermes_shm::SystemInfo scs::Singleton<hermes_shm::SystemInfo>::obj_ = hermes_shm::SystemInfo();
#include <hermes_shm/memory/memory_manager.h>
template<> hermes_shm::ipc::MemoryManager scs::Singleton<hermes_shm::ipc::MemoryManager>::obj_ = hermes_shm::ipc::MemoryManager();
#include <hermes_shm/thread/thread_manager.h>
template<> hermes_shm::ThreadManager scs::Singleton<hermes_shm::ThreadManager>::obj_ = hermes_shm::ThreadManager();
