# Shared Memory Ring Buffer Design

> **Created by Cline** for team review

**Status:** Draft  
**Date:** 2026-05-02  
**C++ Version:** C++23 minimum, C++26 features optional with fallback  
**Target:** `core/messaging/shm_ring.hpp`

---

## C++ Version Requirements

This design targets **C++23** as the minimum version, with optional **C++26** features:

| Feature | C++ Version | Fallback |
|---------|-------------|----------|
| `std::atomic` | C++11 | N/A |
| `std::hardware_destructive_interference_size` | C++17 | `constexpr size_t = 64` |
| `std::expected<T,E>` | C++23 | `std::variant<T, Error>` or custom struct |
| `consteval` | C++20 | `constexpr` (runtime evaluation allowed) |
| `std::generator` | C++26 (optional) | Custom iterator |
| `std::execution` | C++17 | Sequential execution |
| Modules | C++20 (optional) | Traditional headers |

**Note:** While `std::atomic` is available since C++11, we use C++23 features like `std::atomic::wait` if available.

**Compiler Requirements:**
- Clang 17+ / GCC 13+ minimum
- Clang 20+ / GCC 15+ for C++26 features
- No exceptions on hot path (`-fno-exceptions`)

---

## Overview

This document specifies the lock-free SPSC (Single Producer Single Consumer) shared memory ring buffer for the OnTrade hot path. The design prioritizes:

1. **Latency:** < 100 ns per hop (50-200 ns target)
2. **Determinism:** No locks, no syscalls, no allocations
3. **FPGA Compatibility:** Fixed memory layout, simple protocol
4. **Observability:** Sequence numbers for gap detection

---

## Design Principles

### 1. Cache Line Alignment (64 bytes)

```
Cache Line 0: [Producer Sequence | Consumer Sequence | Padding]
Cache Line 1: [Slot 0 Data (64 bytes)]
Cache Line 2: [Slot 1 Data (64 bytes)]
...
Cache Line N: [Slot N-1 Data (64 bytes)]
```

**Why:** Prevents false sharing between producer and consumer.

### 2. Power-of-2 Ring Size

- Ring size: 2^N slots (e.g., 4096 = 2^12)
- Mask: `index = sequence & (size - 1)`
- No modulo operation

### 3. Separate Sequences (Disruptor Pattern)

- **Producer sequence:** Next slot to write
- **Consumer sequence:** Last slot read
- **Claim/Commit:** Producer claims slot, writes data, commits
- **Wait/Read:** Consumer waits for commit, reads, advances

### 4. Memory Ordering

- **Producer:** `release` on sequence write (commit)
- **Consumer:** `acquire` on sequence read (wait)
- **No `seq_cst`:** Not needed for SPSC

---

## Memory Layout

```cpp
// shm_ring.hpp
#pragma once

#include <atomic>
#include <cstdint>
#include <emmintrin.h>  // _mm_pause

namespace ontrade::core::messaging {

// Cache line size (x86_64)
// C++23: std::hardware_destructive_interference_size
// Fallback for C++20: constexpr
inline constexpr size_t kCacheLineSize = 64;

// Ring header - cache line aligned
// NOTE: This struct is designed to be initialized via placement new
// The const members are set once at initialization and never modified
struct alignas(kCacheLineSize) RingHeader {
    // Producer sequence - written by producer, read by consumer
    // Aligned to separate cache line to prevent false sharing
    alignas(kCacheLineSize) std::atomic<uint64_t> producer_sequence{0};
    
    // Consumer sequence - written by consumer, read by producer
    // Aligned to separate cache line to prevent false sharing
    alignas(kCacheLineSize) std::atomic<uint64_t> consumer_sequence{0};
    
    // Ring configuration (immutable after init)
    // These are set via placement new in ShmRing::init()
    uint64_t slot_size;      // Size of each slot (power of 2, >= 64)
    uint64_t slot_count;     // Number of slots (power of 2)
    uint64_t slot_mask;      // slot_count - 1 (for fast modulo)
    
    // Padding to ensure header is multiple of cache lines
    // Header size: 2 cache lines (producer + consumer) + config
    char padding[kCacheLineSize - 3 * sizeof(uint64_t)];
    
    // Data follows immediately after header
    // char data[slot_count * slot_size];
};

static_assert(sizeof(RingHeader) == 3 * kCacheLineSize, 
              "RingHeader must be 3 cache lines to separate producer/consumer sequences");

// Slot state (first byte of each slot)
enum class SlotState : uint8_t {
    Empty = 0,      // Ready for producer
    Claimed = 1,  // Producer owns, writing
    Committed = 2 // Ready for consumer
};

// Slot header (first 8 bytes of each slot)
struct SlotHeader {
    SlotState state;
    uint8_t padding[7];  // Align to 8 bytes
};

static_assert(sizeof(SlotHeader) == 8);

} // namespace ontrade::core::messaging
```

---

## Producer Implementation

```cpp
// shm_ring.hpp (continued)

template <size_t SlotSize = 64, size_t SlotCount = 4096>
class ShmRingProducer {
    static_assert(SlotSize >= 64, "Slot size must be at least cache line");
    static_assert((SlotSize & (SlotSize - 1)) == 0, "Slot size must be power of 2");
    static_assert((SlotCount & (SlotCount - 1)) == 0, "Slot count must be power of 2");
    
public:
    explicit ShmRingProducer(RingHeader* header) : header_(header) {
        data_ = reinterpret_cast<char*>(header + 1);
    }
    
    // Non-copyable, non-movable
    ShmRingProducer(const ShmRingProducer&) = delete;
    ShmRingProducer& operator=(const ShmRingProducer&) = delete;
    
    // Claim a slot for writing
    // Returns nullptr if ring is full
    [[nodiscard]] void* try_claim() noexcept {
        const uint64_t seq = header_->producer_sequence.load(std::memory_order_relaxed);
        const uint64_t next_seq = seq + 1;
        
        // Check if consumer has advanced enough
        const uint64_t consumer_seq = header_->consumer_sequence.load(std::memory_order_acquire);
        if (next_seq - consumer_seq > SlotCount) {
            return nullptr;  // Ring full
        }
        
        // Claim the slot
        const size_t index = seq & (SlotCount - 1);
        char* slot = data_ + (index * SlotSize);
        
        auto* slot_header = reinterpret_cast<SlotHeader*>(slot);
        slot_header->state = SlotState::Claimed;
        
        // Return pointer to payload (after slot header)
        return slot + sizeof(SlotHeader);
    }
    
    // Commit the claimed slot
    // Must be called after writing to the slot
    void commit() noexcept {
        const uint64_t seq = header_->producer_sequence.load(std::memory_order_relaxed);
        const size_t index = seq & (SlotCount - 1);
        char* slot = data_ + (index * SlotSize);
        
        // Mark as committed
        auto* slot_header = reinterpret_cast<SlotHeader*>(slot);
        slot_header->state = SlotState::Committed;
        
        // Advance sequence (release to consumer)
        header_->producer_sequence.store(seq + 1, std::memory_order_release);
    }
    
    // Blocking claim with spin
    [[nodiscard]] void* claim_spin(int max_spins = 1000) noexcept {
        void* slot = try_claim();
        int spins = 0;
        while (slot == nullptr && spins < max_spins) {
            _mm_pause();
            slot = try_claim();
            ++spins;
        }
        return slot;
    }
    
private:
    RingHeader* header_;
    char* data_;
};
```

---

## Consumer Implementation

```cpp
// shm_ring.hpp (continued)

template <size_t SlotSize = 64, size_t SlotCount = 4096>
class ShmRingConsumer {
public:
    explicit ShmRingConsumer(RingHeader* header) : header_(header) {
        data_ = reinterpret_cast<char*>(header + 1);
    }
    
    // Non-copyable, non-movable
    ShmRingConsumer(const ShmRingConsumer&) = delete;
    ShmRingConsumer& operator=(const ShmRingConsumer&) = delete;
    
    // Try to read next slot
    // Returns nullptr if no data available
    [[nodiscard]] const void* try_read() noexcept {
        const uint64_t consumer_seq = header_->consumer_sequence.load(std::memory_order_relaxed);
        const uint64_t producer_seq = header_->producer_sequence.load(std::memory_order_acquire);
        
        if (consumer_seq >= producer_seq) {
            return nullptr;  // No data
        }
        
        const size_t index = consumer_seq & (SlotCount - 1);
        const char* slot = data_ + (index * SlotSize);
        
        auto* slot_header = reinterpret_cast<const SlotHeader*>(slot);
        if (slot_header->state != SlotState::Committed) {
            return nullptr;  // Not committed yet (shouldn't happen with proper barriers)
        }
        
        // Return pointer to payload
        return slot + sizeof(SlotHeader);
    }
    
    // Advance consumer sequence after processing
    void advance() noexcept {
        const uint64_t seq = header_->consumer_sequence.load(std::memory_order_relaxed);
        
        // Mark slot as empty (optional - for debugging)
        const size_t index = seq & (SlotCount - 1);
        char* slot = data_ + (index * SlotSize);
        auto* slot_header = reinterpret_cast<SlotHeader*>(slot);
        slot_header->state = SlotState::Empty;
        
        // Advance (release to producer)
        header_->consumer_sequence.store(seq + 1, std::memory_order_release);
    }
    
    // Blocking read with spin
    [[nodiscard]] const void* read_spin(int max_spins = 10000) noexcept {
        const void* data = try_read();
        int spins = 0;
        while (data == nullptr && spins < max_spins) {
            _mm_pause();
            data = try_read();
            ++spins;
        }
        return data;
    }
    
    // Get current lag (producer - consumer)
    [[nodiscard]] uint64_t lag() const noexcept {
        const uint64_t producer = header_->producer_sequence.load(std::memory_order_acquire);
        const uint64_t consumer = header_->consumer_sequence.load(std::memory_order_relaxed);
        return producer - consumer;
    }
    
private:
    RingHeader* header_;
    char* data_;
};
```

---

## Initialization

```cpp
// shm_ring.hpp (continued)

// Create or open shared memory ring
class ShmRing {
public:
    // Create new ring in existing shared memory
    static void init(void* memory, size_t slot_size = 64, size_t slot_count = 4096) {
        auto* header = static_cast<RingHeader*>(memory);
        
        // Initialize header
        new (header) RingHeader{
            .slot_size = slot_size,
            .slot_count = slot_count,
            .slot_mask = slot_count - 1
        };
        
        // Initialize all slots to Empty
        char* data = reinterpret_cast<char*>(header + 1);
        for (size_t i = 0; i < slot_count; ++i) {
            char* slot = data + (i * slot_size);
            auto* slot_header = reinterpret_cast<SlotHeader*>(slot);
            slot_header->state = SlotState::Empty;
        }
    }
    
    // Calculate required memory size
    static constexpr size_t required_size(size_t slot_size, size_t slot_count) {
        return sizeof(RingHeader) + (slot_size * slot_count);
    }
};

} // namespace ontrade::core::messaging
```

---

## Usage Example

```cpp
#include "core/messaging/shm_ring.hpp"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

using namespace ontrade::core::messaging;

// Message payload (must fit in slot)
struct MarketDataEvent {
    uint64_t sequence;
    uint64_t timestamp;
    uint32_t symbol_id;
    uint32_t price;
    uint32_t quantity;
    uint8_t type;
    uint8_t flags;
    // Padding to fill slot
    char padding[64 - 29];
};
static_assert(sizeof(MarketDataEvent) == 64);

// Producer process
void producer() {
    // Open shared memory
    int fd = shm_open("/ontrade_md_ring", O_CREAT | O_RDWR, 0666);
    size_t size = ShmRing::required_size(64, 4096);
    ftruncate(fd, size);
    
    void* memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ShmRing::init(memory, 64, 4096);
    
    auto* header = static_cast<RingHeader*>(memory);
    ShmRingProducer<64, 4096> producer(header);
    
    // Produce events
    for (uint64_t i = 0; ; ++i) {
        void* slot = producer.try_claim();
        if (slot) {
            auto* event = static_cast<MarketDataEvent*>(slot);
            event->sequence = i;
            event->timestamp = get_hardware_timestamp();
            event->symbol_id = 12345;
            event->price = 100000000;  // $100.00
            event->quantity = 100;
            event->type = 1;  // Trade
            producer.commit();
        } else {
            // Ring full - backpressure
            _mm_pause();
        }
    }
}

// Consumer process
void consumer() {
    int fd = shm_open("/ontrade_md_ring", O_RDWR, 0666);
    size_t size = ShmRing::required_size(64, 4096);
    
    void* memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    auto* header = static_cast<RingHeader*>(memory);
    
    ShmRingConsumer<64, 4096> consumer(header);
    
    while (true) {
        const void* slot = consumer.try_read();
        if (slot) {
            const auto* event = static_cast<const MarketDataEvent*>(slot);
            process_event(*event);
            consumer.advance();
        } else {
            // No data - spin
            _mm_pause();
        }
    }
}
```

---

## Performance Characteristics

| Metric | Target | Notes |
|--------|--------|-------|
| **Latency (empty ring)** | ~50 ns | Claim + commit |
| **Latency (contended)** | ~100-200 ns | With consumer spinning |
| **Throughput** | > 10M msg/s | Per ring |
| **Cache Misses** | 2 per message | Header + data |
| **False Sharing** | 0 | Separate cache lines |

---

## Backoff Strategies

```cpp
// Exponential backoff with yield
class Backoff {
public:
    void wait() {
        if (count_ < 10) {
            // Spin with pause
            for (int i = 0; i < (1 << count_); ++i) {
                _mm_pause();
            }
        } else if (count_ < 20) {
            // Yield to OS
            sched_yield();
        } else {
            // Sleep briefly (not for hot path)
            timespec ts{0, 1000};  // 1 µs
            nanosleep(&ts, nullptr);
        }
        ++count_;
    }
    
    void reset() { count_ = 0; }
    
private:
    int count_ = 0;
};
```

**Hot path:** Never use backoff - dedicated cores, always spin.  
**Non-hot path:** Use exponential backoff to reduce CPU usage.

---

## FPGA Compatibility

The ring layout is designed for FPGA implementation:

```verilog
// Verilog pseudo-code for FPGA ring reader
module shm_ring_reader (
    input  [63:0] producer_seq,
    input  [63:0] consumer_seq,
    input  [63:0] slot_data,
    output [63:0] next_consumer_seq,
    output        data_valid
);
    // Simple comparison - no complex state machine
    assign data_valid = (producer_seq > consumer_seq);
    assign next_consumer_seq = consumer_seq + 1;
endmodule
```

**Key FPGA-friendly features:**
- Fixed offsets (no pointers)
- Simple state machine (Empty → Claimed → Committed)
- No locks or atomics needed (FPGA is single-threaded)
- Power-of-2 sizes (bit masking)

---

## Testing

```cpp
// shm_ring_test.cpp
#include <gtest/gtest.h>

TEST(ShmRing, BasicRoundTrip) {
    alignas(4096) char memory[ShmRing::required_size(64, 16)];
    ShmRing::init(memory, 64, 16);
    
    auto* header = reinterpret_cast<RingHeader*>(memory);
    ShmRingProducer<64, 16> producer(header);
    ShmRingConsumer<64, 16> consumer(header);
    
    // Produce
    for (int i = 0; i < 100; ++i) {
        void* slot = producer.claim_spin();
        ASSERT_NE(slot, nullptr);
        *static_cast<uint64_t*>(slot) = i;
        producer.commit();
    }
    
    // Consume
    for (int i = 0; i < 100; ++i) {
        const void* slot = consumer.read_spin();
        ASSERT_NE(slot, nullptr);
        EXPECT_EQ(*static_cast<const uint64_t*>(slot), i);
        consumer.advance();
    }
}

TEST(ShmRing, FullRing) {
    // Test backpressure when ring is full
}

TEST(ShmRing, FalseSharing) {
    // Verify producer/consumer on different cache lines
}
```

---

## Open Questions

1. **Slot Size:** Fixed 64 bytes or variable with header? (Proposed: fixed for cache efficiency)
2. **Multiple Consumers:** MPSC variant needed? (Proposed: separate ring per consumer)
3. **Batching:** Support multi-slot claim/commit? (Proposed: no, keep simple)
4. **Persistence:** Integration with Aeron archiver? (Proposed: Aeron for archive, this for hot path)

---

## References

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
- [Aeron IPC](https://github.com/real-logic/aeron)
- Architecture §5.3: Critical-path patterns
- Architecture §5.5: Shared-memory bus
- Architecture §5.4: C++ standard policy

---

*This design document was created by **Cline** for team review*
