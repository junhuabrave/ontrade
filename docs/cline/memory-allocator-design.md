# Memory Allocator Design for Hot Path

> **Created by Cline** for team review

**Status:** Draft  
**Date:** 2026-05-02  
**C++ Version:** C++23 minimum  
**Target:** `core/memory/arena.hpp`, `core/memory/pool.hpp`

---

## Overview

The architecture (§5.3) mandates **zero allocations on the order path**. This document specifies the memory allocation strategy for the C++ hot path, comparing:

1. **Custom Arena Allocator** (Recommended)
2. **Boost.Singleton_Pool** (Alternative)
3. **Intel TBB Concurrent Queue** (Alternative)

**Decision:** Use custom arena allocator for hot path; Boost/TBB only if profiling shows need.

---

## Requirements

| Requirement | Target | Rationale |
|-------------|--------|-----------|
| **Zero allocations on hot path** | Mandatory | Architecture §5.3 |
| **Deterministic performance** | No variance | Latency SLOs |
| **No locks** | Lock-free only | Contention kills latency |
| **Cache-friendly** | Sequential access | Better prefetching |
| **O(1) allocation** | Constant time | Predictable latency |
| **Thread-safe** | Per-thread arenas | No cross-thread contention |
| **CI-time verification** | Build fails on allocation | Enforcement |

---

## Option 1: Custom Arena Allocator (Recommended)

### Design

```cpp
// core/memory/arena.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

namespace ontrade::core::memory {

// Bump allocator - arena-style
class ArenaAllocator {
public:
    // Constructor with pre-allocated buffer
    explicit ArenaAllocator(void* buffer, size_t size) noexcept
        : buffer_(static_cast<char*>(buffer))
        , size_(size)
        , offset_(0) {}
    
    // Non-copyable, movable
    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) = default;
    ArenaAllocator& operator=(ArenaAllocator&&) = default;
    
    // Allocate - O(1), no locks, no exceptions
    [[nodiscard]] void* allocate(size_t size, size_t alignment = alignof(std::max_align_t)) noexcept {
        // Align offset
        size_t aligned_offset = (offset_ + alignment - 1) & ~(alignment - 1);
        
        // Check bounds
        if (aligned_offset + size > size_) {
            return nullptr;  // Out of memory
        }
        
        void* ptr = buffer_ + aligned_offset;
        offset_ = aligned_offset + size;
        return ptr;
    }
    
    // Allocate with type safety
    template <typename T, typename... Args>
    [[nodiscard]] T* construct(Args&&... args) noexcept {
        void* ptr = allocate(sizeof(T), alignof(T));
        if (!ptr) return nullptr;
        return new (ptr) T(std::forward<Args>(args)...);
    }
    
    // Reset - O(1), invalidates all allocations
    void reset() noexcept {
        offset_ = 0;
    }
    
    // Current usage
    [[nodiscard]] size_t used() const noexcept { return offset_; }
    [[nodiscard]] size_t available() const noexcept { return size_ - offset_; }
    [[nodiscard]] size_t capacity() const noexcept { return size_; }
    
private:
    char* buffer_;
    size_t size_;
    size_t offset_;
};

// Scoped arena - auto-reset on destruction
class ScopedArena {
public:
    explicit ScopedArena(ArenaAllocator& arena) : arena_(arena), saved_offset_(arena.used()) {}
    ~ScopedArena() { arena_.reset(); }
    
    ArenaAllocator& get() { return arena_; }
    
private:
    ArenaAllocator& arena_;
    size_t saved_offset_;
};

} // namespace ontrade::core::memory
```

### Per-Session Arena

**IMPORTANT:** SessionArena must be initialized at process startup, NOT on the hot path.
The buffer is allocated once during initialization, then reused for the entire session.

```cpp
// Session-scoped arena for packet buffers
// NOTE: This allocates during construction - must be created at startup, not on hot path
class SessionArena {
public:
    // Constructor allocates memory - call at startup only
    explicit SessionArena(size_t size) 
        : buffer_(std::make_unique<char[]>(size))
        , arena_(buffer_.get(), size) {}
    
    // Non-copyable, movable
    SessionArena(const SessionArena&) = delete;
    SessionArena& operator=(const SessionArena&) = delete;
    SessionArena(SessionArena&&) = default;
    SessionArena& operator=(SessionArena&&) = default;
    
    ArenaAllocator& get() { return arena_; }
    
    // Reset at end of session (does not allocate)
    void reset() { arena_.reset(); }
    
private:
    std::unique_ptr<char[]> buffer_;  // Allocated at startup
    ArenaAllocator arena_;            // Embedded, not heap-allocated
};

// Thread-local session arena - initialized at thread startup
thread_local std::unique_ptr<SessionArena> g_session_arena;

// Call this at thread startup, NOT on hot path
void init_session_arena(size_t size) {
    g_session_arena = std::make_unique<SessionArena>(size);
}

// Hot path - no allocation
ArenaAllocator& get_session_arena() {
    return g_session_arena->get();
}
```

**Critical Design Point:**
- `init_session_arena()` allocates memory - call once at thread startup
- `get_session_arena()` is allocation-free - safe for hot path
- `SessionArena` embeds `ArenaAllocator` (not pointer) to avoid extra allocation

### Object Pool (for reusable objects)

```cpp
// core/memory/pool.hpp
#pragma once

#include <array>
#include <atomic>

namespace ontrade::core::memory {

// Lock-free object pool
// Fixed capacity, pre-allocated
// O(1) acquire/release
// Single producer, single consumer (SPSC) per thread

template <typename T, size_t Capacity>
class ObjectPool {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
    
public:
    ObjectPool() {
        // Initialize free list
        for (size_t i = 0; i < Capacity; ++i) {
            free_list_[i] = &storage_[i];
        }
        head_.store(0, std::memory_order_relaxed);
        tail_.store(Capacity, std::memory_order_relaxed);
    }
    
    // Acquire object - O(1)
    [[nodiscard]] T* acquire() noexcept {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t tail = tail_.load(std::memory_order_acquire);
        
        if (head == tail) {
            return nullptr;  // Pool empty
        }
        
        T* obj = free_list_[head & (Capacity - 1)];
        head_.store(head + 1, std::memory_order_release);
        return obj;
    }
    
    // Release object - O(1)
    void release(T* obj) noexcept {
        size_t tail = tail_.load(std::memory_order_relaxed);
        
        free_list_[tail & (Capacity - 1)] = obj;
        tail_.store(tail + 1, std::memory_order_release);
    }
    
    // Construct and acquire
    template <typename... Args>
    [[nodiscard]] T* construct(Args&&... args) noexcept {
        T* obj = acquire();
        if (obj) {
            new (obj) T(std::forward<Args>(args)...);
        }
        return obj;
    }
    
    // Destroy and release
    void destroy(T* obj) noexcept {
        if (obj) {
            obj->~T();
            release(obj);
        }
    }
    
private:
    alignas(64) std::array<T, Capacity> storage_;
    alignas(64) std::array<T*, Capacity> free_list_;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
};

} // namespace ontrade::core::memory
```

### Pros

| Aspect | Benefit |
|--------|---------|
| **Zero dependencies** | No Boost, no TBB |
| **Predictable** | O(1) always, no variance |
| **Cache-friendly** | Sequential allocation |
| **Simple** | Easy to understand, debug |
| **CI-verifiable** | Can track all allocations |
| **No locks** | Single-threaded per arena |

### Cons

| Aspect | Cost |
|--------|------|
| **No deallocation** | Must reset arena (session boundary) |
| **Fixed size** | Must pre-allocate max needed |
| **Manual management** | Developer must track lifetimes |
| **No thread sharing** | Per-thread arenas required |

---

## Option 2: Boost.Singleton_Pool

### Design

```cpp
// Using Boost.Pool
#include <boost/pool/singleton_pool.hpp>

struct PacketPoolTag {};
using PacketPool = boost::singleton_pool<PacketPoolTag, 1024>;

// Usage
void* packet = PacketPool::malloc();  // O(1) typically
PacketPool::free(packet);
```

### Pros

| Aspect | Benefit |
|--------|---------|
| **Mature** | Well-tested, widely used |
| **Fast** | O(1) allocation/deallocation |
| **Thread-safe** | Built-in locking (or lock-free variants) |
| **Flexible** | Can free individual objects |

### Cons

| Aspect | Cost |
|--------|------|
| **Dependency** | Boost dependency in hot path |
| **Singleton** | Global state, testing issues |
| **Locking** | Thread-safe version has contention |
| **Complexity** | More complex than arena |
| **Memory overhead** | Free list overhead per pool |

### Verdict

**Rejected for hot path.** Acceptable for control plane if needed.

---

## Option 3: Intel TBB Concurrent Queue

### Design

```cpp
#include <tbb/concurrent_queue.h>

tbb::concurrent_queue<Packet*> packet_pool;

// Initialize
for (int i = 0; i < 1000; ++i) {
    packet_pool.push(new Packet());
}

// Usage
Packet* packet;
if (packet_pool.try_pop(packet)) {
    // Use packet
    packet_pool.push(packet);  // Return to pool
}
```

### Pros

| Aspect | Benefit |
|--------|---------|
| **Lock-free** | Wait-free for SPSC |
| **Scalable** | Intel optimized |
| **Flexible** | Multiple producers/consumers |

### Cons

| Aspect | Cost |
|--------|------|
| **Dependency** | TBB is large dependency |
| **Overkill** | SPSC doesn't need MPMC |
| **Pointer chasing** | Less cache-friendly than arena |
| **Allocation tracking** | Harder to verify zero-allocation |

### Verdict

**Rejected.** Custom solution is simpler and faster for SPSC.

---

## Allocation Tracking (CI Enforcement)

```cpp
// core/memory/tracker.hpp
#pragma once

#include <atomic>
#include <cstdlib>

namespace ontrade::core::memory {

// Global allocation counter for CI
inline std::atomic<size_t> g_allocation_count{0};

// Override new/delete for tracking
struct AllocationTracker {
    static void* allocate(size_t size) {
        g_allocation_count.fetch_add(1, std::memory_order_relaxed);
        return std::malloc(size);
    }
    
    static void deallocate(void* ptr) {
        std::free(ptr);
    }
};

// Scoped guard - fails test if allocations occur
class AllocationGuard {
public:
    AllocationGuard() : start_count_(g_allocation_count.load()) {}
    
    ~AllocationGuard() {
        size_t end_count = g_allocation_count.load();
        if (end_count != start_count_) {
            // In CI: fail the build
            // In debug: log warning
            std::abort();  // Or throw in test mode
        }
    }
    
    [[nodiscard]] bool allocations_occurred() const {
        return g_allocation_count.load() != start_count_;
    }
    
private:
    size_t start_count_;
};

} // namespace ontrade::core::memory

// Override global new/delete in test builds
#ifdef ONTRADE_TRACK_ALLOCATIONS
void* operator new(size_t size) {
    return ontrade::core::memory::AllocationTracker::allocate(size);
}

void operator delete(void* ptr) noexcept {
    ontrade::core::memory::AllocationTracker::deallocate(ptr);
}
#endif
```

### CI Integration

```cmake
# CMakeLists.txt
option(ONTRADE_TRACK_ALLOCATIONS "Track allocations in tests" ON)

if(ONTRADE_TRACK_ALLOCATIONS)
    target_compile_definitions(core PRIVATE ONTRADE_TRACK_ALLOCATIONS)
    target_link_options(core PRIVATE -Wl,--wrap,malloc -Wl,--wrap,free)
endif()

# Test
add_test(NAME NoAllocationsOnHotPath 
    COMMAND hot_path_test --allocation-guard)
```

---

## Usage Patterns

### Pattern 1: Session-Scoped Packet Buffers

```cpp
class MdIngress {
public:
    void on_session_start() {
        // Pre-allocate all needed memory
        session_arena_ = std::make_unique<SessionArena>(100 * 1024 * 1024);  // 100MB
    }
    
    void on_packet(const char* data, size_t len) {
        // Allocate from arena - O(1), no locks
        void* buffer = session_arena_->get().allocate(len);
        if (!buffer) {
            // Handle out-of-memory (shouldn't happen with proper sizing)
            return;
        }
        
        std::memcpy(buffer, data, len);
        process_packet(buffer, len);
    }
    
    void on_session_end() {
        // Reset arena - invalidates all allocations
        session_arena_->reset();
    }
    
private:
    std::unique_ptr<SessionArena> session_arena_;
};
```

### Pattern 2: Object Pool for Orders

```cpp
class OMS {
public:
    OMS() {
        // Pre-construct order objects
        for (int i = 0; i < 10000; ++i) {
            order_pool_.construct();
        }
    }
    
    Order* new_order(const OrderRequest& req) {
        Order* order = order_pool_.acquire();
        if (!order) {
            // Pool exhausted - should size appropriately
            return nullptr;
        }
        
        // Initialize from request
        order->cl_ord_id = req.cl_ord_id;
        order->symbol = req.symbol;
        // ...
        
        return order;
    }
    
    void cancel_order(Order* order) {
        // Cleanup
        order->~Order();
        order_pool_.release(order);
    }
    
private:
    ObjectPool<Order, 10000> order_pool_;
};
```

### Pattern 3: Scoped Arena for Processing

```cpp
void process_message(const Message& msg) {
    // Scoped arena auto-resets on scope exit
    ScopedArena scope(get_thread_arena());
    ArenaAllocator& arena = scope.get();
    
    // All allocations use arena
    auto* temp_buffer = arena.allocate(msg.size());
    auto* parsed = arena.construct<ParsedMessage>(msg);
    
    // Process...
    
    // Arena auto-resets here - no manual cleanup needed
}
```

---

## Sizing Guidelines

| Component | Size | Rationale |
|-----------|------|-----------|
| **Session arena** | 100-500 MB | Per-session, reset at EOD |
| **Order pool** | 10K-100K objects | Max open orders * 2 |
| **Packet buffers** | 1-4 MB | Burst absorption |
| **Thread stack** | 8 MB | Default, pinned to NUMA node |

**Sizing formula:**
```
SessionArena = (MaxMsgRate * AvgMsgSize * MaxBurstSeconds) * 2
```

Example: 1M msg/s * 64 bytes * 10s burst * 2 = 1.28 GB → Round to 2 GB

---

## NUMA Considerations

```cpp
#include <numa.h>

// Allocate on specific NUMA node
void* numa_alloc(size_t size, int node) {
    // numa_alloc_onnode returns void* on success, NULL on failure
    void* ptr = numa_alloc_onnode(size, node);
    if (!ptr) {
        // Fallback to regular allocation
        ptr = std::aligned_alloc(4096, size);
    }
    return ptr;
}

// Pin thread to NUMA node
void pin_to_numa(int node) {
    if (numa_available() >= 0) {
        numa_run_on_node(node);
    }
}

// Usage - call at startup, NOT on hot path
void init_arena_on_node(int node, size_t arena_size) {
    pin_to_numa(node);
    void* buffer = numa_alloc(arena_size, node);
    if (!buffer) {
        throw std::runtime_error("Failed to allocate NUMA memory");
    }
    arena = std::make_unique<ArenaAllocator>(buffer, arena_size);
}
```

**NUMA Best Practices:**
- Allocate memory on the same NUMA node as the thread that will use it
- Pin threads to NUMA nodes to prevent migration
- Use `numa_alloc_onnode()` for explicit NUMA allocation
- Fall back to `std::aligned_alloc()` if NUMA allocation fails
- Always check `numa_available()` before using NUMA functions

---

## Testing

```cpp
// arena_test.cpp
#include <gtest/gtest.h>

TEST(ArenaAllocator, BasicAllocation) {
    alignas(64) char buffer[1024];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    void* p1 = arena.allocate(100);
    ASSERT_NE(p1, nullptr);
    
    void* p2 = arena.allocate(100);
    ASSERT_NE(p2, nullptr);
    ASSERT_NE(p1, p2);
    
    arena.reset();
    
    // After reset, same addresses
    void* p3 = arena.allocate(100);
    ASSERT_EQ(p3, p1);
}

TEST(ArenaAllocator, Alignment) {
    alignas(64) char buffer[1024];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    void* p = arena.allocate(1, 64);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(p) % 64, 0);
}

TEST(ArenaAllocator, OutOfMemory) {
    alignas(64) char buffer[100];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    void* p1 = arena.allocate(50);
    ASSERT_NE(p1, nullptr);
    
    void* p2 = arena.allocate(100);  // Too big
    ASSERT_EQ(p2, nullptr);
}

TEST(ObjectPool, AcquireRelease) {
    ObjectPool<int, 10> pool;
    
    int* p1 = pool.acquire();
    ASSERT_NE(p1, nullptr);
    
    *p1 = 42;
    pool.release(p1);
    
    int* p2 = pool.acquire();
    ASSERT_EQ(p2, p1);  // Reused
}

TEST(HotPath, NoAllocations) {
    AllocationGuard guard;
    
    // Run hot path code
    process_market_data();
    
    ASSERT_FALSE(guard.allocations_occurred());
}
```

---

## Open Questions

1. **Arena sizing:** Static sizing vs dynamic growth? (Proposed: static with monitoring)
2. **OOM handling:** Crash vs graceful degradation? (Proposed: crash in dev, degrade in prod)
3. **Cross-arena sharing:** Allow objects to move between arenas? (Proposed: no)
4. **Debug builds:** Add allocation tracking overhead? (Proposed: yes, with compile flag)

---

## References

- [Boost.Pool](https://www.boost.org/doc/libs/release/libs/pool/doc/html/index.html)
- [Intel TBB](https://github.com/oneapi-src/oneTBB)
- [jemalloc](http://jemalloc.net/)
- Architecture §5.3: Zero allocations on the order path
- Architecture §5.4: C++ standard policy

---

*This design document was created by **Cline** for team review*
