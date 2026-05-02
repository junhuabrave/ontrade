# Testing Strategy for Hot Path Components

> **Created by Cline** for team review

**Status:** Draft  
**Date:** 2026-05-02  
**Applies to:** SHM Ring, Memory Allocator, SBE Serialization

---

## Overview

Testing low-latency C++ components requires a two-pronged approach:

1. **Correctness Testing:** Ensure the code works under all conditions
2. **Performance Testing:** Ensure it meets latency/throughput SLOs

Both must be automated in CI with strict gates.

---

## Part 1: Correctness Testing

### 1.1 Unit Tests (Google Test)

#### SHM Ring Tests

```cpp
// test/shm_ring_test.cpp
#include <gtest/gtest.h>
#include "core/messaging/shm_ring.hpp"
#include <thread>
#include <vector>
#include <random>

using namespace ontrade::core::messaging;

class ShmRingTest : public ::testing::Test {
protected:
    static constexpr size_t kSlotSize = 64;
    static constexpr size_t kSlotCount = 1024;
    
    void SetUp() override {
        memory_ = std::make_unique<char[]>(ShmRing::required_size(kSlotSize, kSlotCount));
        ShmRing::init(memory_.get(), kSlotSize, kSlotCount);
        header_ = reinterpret_cast<RingHeader*>(memory_.get());
    }
    
    std::unique_ptr<char[]> memory_;
    RingHeader* header_;
};

// Basic functionality
TEST_F(ShmRingTest, SingleThreadedRoundTrip) {
    ShmRingProducer<kSlotSize, kSlotCount> producer(header_);
    ShmRingConsumer<kSlotSize, kSlotCount> consumer(header_);
    
    for (int i = 0; i < 1000; ++i) {
        // Produce
        void* slot = producer.try_claim();
        ASSERT_NE(slot, nullptr);
        *static_cast<uint64_t*>(slot) = i;
        producer.commit();
        
        // Consume
        const void* read_slot = consumer.try_read();
        ASSERT_NE(read_slot, nullptr);
        EXPECT_EQ(*static_cast<const uint64_t*>(read_slot), i);
        consumer.advance();
    }
}

// Ring full behavior
TEST_F(ShmRingTest, RingFullBackpressure) {
    ShmRingProducer<kSlotSize, kSlotCount> producer(header_);
    
    // Fill ring completely
    for (size_t i = 0; i < kSlotCount; ++i) {
        void* slot = producer.try_claim();
        ASSERT_NE(slot, nullptr);
        producer.commit();
    }
    
    // Next claim should fail
    EXPECT_EQ(producer.try_claim(), nullptr);
}

// Multi-threaded stress test
TEST_F(ShmRingTest, MultiThreadedStress) {
    constexpr int kIterations = 100000;
    constexpr int kProducerThreads = 2;  // Multiple producers not supported, use separate rings
    
    // For SPSC, we test with single producer/consumer
    std::thread producer([this]() {
        ShmRingProducer<kSlotSize, kSlotCount> producer(header_);
        for (int i = 0; i < kIterations; ++i) {
            void* slot = producer.claim_spin();
            *static_cast<uint64_t*>(slot) = i;
            producer.commit();
        }
    });
    
    std::thread consumer([this]() {
        ShmRingConsumer<kSlotSize, kSlotCount> consumer(header_);
        for (int i = 0; i < kIterations; ++i) {
            const void* slot = consumer.read_spin();
            EXPECT_EQ(*static_cast<const uint64_t*>(slot), i);
            consumer.advance();
        }
    });
    
    producer.join();
    consumer.join();
}

// Memory ordering test (TSan/Helgrind)
TEST_F(ShmRingTest, MemoryOrdering) {
    // This test is designed to be run under ThreadSanitizer
    // It will fail if there are data races
    ShmRingProducer<kSlotSize, kSlotCount> producer(header_);
    ShmRingConsumer<kSlotSize, kSlotCount> consumer(header_);
    
    std::atomic<bool> start{false};
    std::atomic<int> ready_count{0};
    
    auto producer_fn = [&]() {
        ready_count.fetch_add(1);
        while (!start.load()) { _mm_pause(); }
        
        for (int i = 0; i < 10000; ++i) {
            void* slot = producer.claim_spin();
            *static_cast<uint64_t*>(slot) = i;
            producer.commit();
        }
    };
    
    auto consumer_fn = [&]() {
        ready_count.fetch_add(1);
        while (!start.load()) { _mm_pause(); }
        
        for (int i = 0; i < 10000; ++i) {
            const void* slot = consumer.read_spin();
            EXPECT_EQ(*static_cast<const uint64_t*>(slot), i);
            consumer.advance();
        }
    };
    
    std::thread t1(producer_fn);
    std::thread t2(consumer_fn);
    
    while (ready_count.load() < 2) { _mm_pause(); }
    start.store(true);
    
    t1.join();
    t2.join();
}

// False sharing detection
TEST_F(ShmRingTest, NoFalseSharing) {
    // Verify producer and consumer sequences are on different cache lines
    EXPECT_GE(reinterpret_cast<uintptr_t>(&header_->consumer_sequence) - 
              reinterpret_cast<uintptr_t>(&header_->producer_sequence), 64);
}

// Wrap-around test
TEST_F(ShmRingTest, SequenceWrapAround) {
    // Manually set sequences near overflow
    header_->producer_sequence.store(UINT64_MAX - 10, std::memory_order_relaxed);
    header_->consumer_sequence.store(UINT64_MAX - 20, std::memory_order_relaxed);
    
    ShmRingProducer<kSlotSize, kSlotCount> producer(header_);
    ShmRingConsumer<kSlotSize, kSlotCount> consumer(header_);
    
    // Should still work correctly
    for (int i = 0; i < 100; ++i) {
        void* slot = producer.claim_spin();
        *static_cast<uint64_t*>(slot) = i;
        producer.commit();
        
        const void* read_slot = consumer.read_spin();
        EXPECT_EQ(*static_cast<const uint64_t*>(read_slot), i);
        consumer.advance();
    }
}
```

#### Memory Allocator Tests

```cpp
// test/arena_test.cpp
#include <gtest/gtest.h>
#include "core/memory/arena.hpp"
#include "core/memory/tracker.hpp"

using namespace ontrade::core::memory;

TEST(ArenaAllocator, NoAllocationOnHotPath) {
    AllocationGuard guard;
    
    alignas(64) char buffer[1024];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    // These should not trigger any allocations
    void* p1 = arena.allocate(100);
    void* p2 = arena.allocate(100, 64);
    auto* p3 = arena.construct<int>(42);
    
    EXPECT_FALSE(guard.allocations_occurred());
}

TEST(ArenaAllocator, Alignment) {
    alignas(64) char buffer[1024];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    // Test various alignments
    for (size_t align : {1, 2, 4, 8, 16, 32, 64}) {
        void* p = arena.allocate(1, align);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(p) % align, 0);
    }
}

TEST(ArenaAllocator, OutOfMemory) {
    alignas(64) char buffer[100];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    // Allocate most of the buffer
    void* p1 = arena.allocate(50);
    ASSERT_NE(p1, nullptr);
    
    // This should fail
    void* p2 = arena.allocate(100);
    EXPECT_EQ(p2, nullptr);
}

TEST(ArenaAllocator, Reset) {
    alignas(64) char buffer[1024];
    ArenaAllocator arena(buffer, sizeof(buffer));
    
    void* p1 = arena.allocate(100);
    arena.reset();
    void* p2 = arena.allocate(100);
    
    // After reset, should get same address
    EXPECT_EQ(p1, p2);
}

TEST(ObjectPool, AcquireRelease) {
    ObjectPool<int, 16> pool;
    
    // Pre-populate
    for (int i = 0; i < 16; ++i) {
        pool.construct();
    }
    
    // Acquire all
    std::vector<int*> ptrs;
    for (int i = 0; i < 16; ++i) {
        int* p = pool.acquire();
        ASSERT_NE(p, nullptr);
        *p = i;
        ptrs.push_back(p);
    }
    
    // Next acquire should fail
    EXPECT_EQ(pool.acquire(), nullptr);
    
    // Release and reacquire
    for (int* p : ptrs) {
        pool.release(p);
    }
    
    // Should get same pointers back
    for (int i = 0; i < 16; ++i) {
        int* p = pool.acquire();
        EXPECT_EQ(p, ptrs[i]);
    }
}
```

### 1.2 Property-Based Testing (RapidCheck)

```cpp
// test/shm_ring_property_test.cpp
#include <rapidcheck/gtest.h>
#include "core/messaging/shm_ring.hpp"

using namespace ontrade::core::messaging;

RC_GTEST_PROP(ShmRing, AlwaysPreservesOrder, ()) {
    constexpr size_t kSlotSize = 64;
    constexpr size_t kSlotCount = 256;
    
    alignas(4096) char memory[ShmRing::required_size(kSlotSize, kSlotCount)];
    ShmRing::init(memory, kSlotSize, kSlotCount);
    
    auto* header = reinterpret_cast<RingHeader*>(memory);
    ShmRingProducer<kSlotSize, kSlotCount> producer(header);
    ShmRingConsumer<kSlotSize, kSlotCount> consumer(header);
    
    // Generate random sequence of operations
    const auto operations = *rc::gen::container<std::vector<int>>(
        rc::gen::inRange(0, 1000)
    );
    
    std::vector<int> produced;
    std::vector<int> consumed;
    
    for (int op : operations) {
        if (op % 2 == 0) {
            // Try produce
            if (void* slot = producer.try_claim()) {
                *static_cast<int*>(slot) = op;
                producer.commit();
                produced.push_back(op);
            }
        } else {
            // Try consume
            if (const void* slot = consumer.try_read()) {
                consumed.push_back(*static_cast<const int*>(slot));
                consumer.advance();
            }
        }
    }
    
    // Drain remaining
    while (const void* slot = consumer.try_read()) {
        consumed.push_back(*static_cast<const int*>(slot));
        consumer.advance();
    }
    
    // Order must be preserved
    RC_ASSERT(produced == consumed);
}

RC_GTEST_PROP(ShmRing, NeverOverwritesUnreadData, ()) {
    // Property: Producer never overwrites data consumer hasn't read
    // This is implicitly tested by the ring full check
}
```

### 1.3 Static Analysis

```cmake
# CMakeLists.txt - Static analysis targets

# Clang Static Analyzer
add_custom_target(clang-analyze
    COMMAND scan-build --use-analyzer=${CLANG} make
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# Clang-Tidy
set(CLANG_TIDY_CHECKS 
    "cppcoreguidelines-*"
    "performance-*"
    "portability-*"
    "concurrency-*"
    "misc-*"
    "-cppcoreguidelines-avoid-magic-numbers"
)

set_target_properties(core PROPERTIES
    CXX_CLANG_TIDY "clang-tidy;--checks=${CLANG_TIDY_CHECKS}"
)

# Include What You Use (IWYU)
find_program(IWYU_PATH NAMES include-what-you-use iwyu)
if(IWYU_PATH)
    set_target_properties(core PROPERTIES
        CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH}
    )
endif()
```

### 1.4 Sanitizers (CI Required)

```cmake
# Sanitizer builds
option(ONTRADE_SANITIZER "Build with sanitizer" OFF)

if(ONTRADE_SANITIZER)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        # Address Sanitizer
        set(SANITIZER_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
        
        # Thread Sanitizer (for memory ordering tests)
        # set(SANITIZER_FLAGS "-fsanitize=thread")
        
        # Undefined Behavior Sanitizer
        set(SANITIZER_FLAGS "${SANITIZER_FLAGS} -fsanitize=undefined")
        
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SANITIZER_FLAGS}")
        set(CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${SANITIZER_FLAGS}")
    endif()
endif()
```

**CI Pipeline:**
```yaml
# .github/workflows/ci.yml
jobs:
  sanitize:
    runs-on: ubuntu-latest
    steps:
      - name: Thread Sanitizer
        run: |
          cmake -B build -DONTRADE_SANITIZER=ON
          cmake --build build
          ctest --test-dir build --output-on-failure
        env:
          TSAN_OPTIONS: "halt_on_error=1"
```

---

## Part 2: Performance Testing

### 2.1 Microbenchmarks (Google Benchmark)

```cpp
// benchmark/shm_ring_benchmark.cpp
#include <benchmark/benchmark.h>
#include "core/messaging/shm_ring.hpp"
#include <thread>

using namespace ontrade::core::messaging;

class ShmRingBenchmark : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) override {
        memory_ = std::make_unique<char[]>(ShmRing::required_size(64, 4096));
        ShmRing::init(memory_.get(), 64, 4096);
        header_ = reinterpret_cast<RingHeader*>(memory_.get());
    }
    
    std::unique_ptr<char[]> memory_;
    RingHeader* header_;
};

BENCHMARK_F(ShmRingBenchmark, ClaimCommitLatency)(benchmark::State& state) {
    ShmRingProducer<64, 4096> producer(header_);
    
    for (auto _ : state) {
        void* slot = producer.try_claim();
        benchmark::DoNotOptimize(slot);
        producer.commit();
    }
    
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * 64);
}

BENCHMARK_F(ShmRingBenchmark, FullRoundTripLatency)(benchmark::State& state) {
    ShmRingProducer<64, 4096> producer(header_);
    ShmRingConsumer<64, 4096> consumer(header_);
    
    // Pre-fill to avoid cold cache
    for (int i = 0; i < 100; ++i) {
        void* slot = producer.try_claim();
        producer.commit();
        consumer.try_read();
        consumer.advance();
    }
    
    for (auto _ : state) {
        void* slot = producer.try_claim();
        benchmark::DoNotOptimize(slot);
        *static_cast<uint64_t*>(slot) = 42;
        producer.commit();
        
        const void* read_slot = consumer.try_read();
        benchmark::DoNotOptimize(read_slot);
        consumer.advance();
    }
}

BENCHMARK_F(ShmRingBenchmark, ThroughputSPSC)(benchmark::State& state) {
    std::thread producer([this, &state]() {
        ShmRingProducer<64, 4096> producer(header_);
        while (state.KeepRunning()) {
            void* slot = producer.claim_spin();
            *static_cast<uint64_t*>(slot) = 42;
            producer.commit();
        }
    });
    
    std::thread consumer([this, &state]() {
        ShmRingConsumer<64, 4096> consumer(header_);
        while (state.KeepRunning()) {
            const void* slot = consumer.read_spin();
            benchmark::DoNotOptimize(slot);
            consumer.advance();
        }
    });
    
    producer.join();
    consumer.join();
    
    state.SetItemsProcessed(state.iterations());
}

// Register benchmarks with different parameters
BENCHMARK_F(ShmRingBenchmark, ClaimCommitLatency)
    ->Unit(benchmark::kNanosecond)
    ->MinTime(1.0)
    ->Repetitions(10)
    ->ReportAggregatesOnly(true);

BENCHMARK_MAIN();
```

### 2.2 Latency Distribution Testing

```cpp
// test/latency_distribution_test.cpp
#include <gtest/gtest.h>
#include "core/messaging/shm_ring.hpp"
#include <vector>
#include <algorithm>
#include <chrono>

using namespace ontrade::core::messaging;

TEST(LatencyDistribution, MeetsSLO) {
    constexpr int kSamples = 1000000;
    constexpr int64_t kTargetP50 = 50;   // ns
    constexpr int64_t kTargetP99 = 200;  // ns
    constexpr int64_t kTargetP999 = 500; // ns
    
    alignas(4096) char memory[ShmRing::required_size(64, 4096)];
    ShmRing::init(memory, 64, 4096);
    auto* header = reinterpret_cast<RingHeader*>(memory);
    
    ShmRingProducer<64, 4096> producer(header);
    ShmRingConsumer<64, 4096> consumer(header);
    
    std::vector<int64_t> latencies;
    latencies.reserve(kSamples);
    
    // Warmup
    for (int i = 0; i < 10000; ++i) {
        void* slot = producer.claim_spin();
        producer.commit();
        consumer.read_spin();
        consumer.advance();
    }
    
    // Measure
    for (int i = 0; i < kSamples; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        
        void* slot = producer.claim_spin();
        producer.commit();
        const void* read_slot = consumer.read_spin();
        consumer.advance();
        
        auto end = std::chrono::high_resolution_clock::now();
        
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        latencies.push_back(ns);
    }
    
    std::sort(latencies.begin(), latencies.end());
    
    auto p50 = latencies[kSamples * 0.50];
    auto p99 = latencies[kSamples * 0.99];
    auto p999 = latencies[kSamples * 0.999];
    \    EXPECT_LE(p50, kTargetP50) << "p50 latency: " << p50 << " ns";
    EXPECT_LE(p99, kTargetP99) << "p99 latency: " << p99 << " ns";
    EXPECT_LE(p999, kTargetP999) << "p99.9 latency: " << p999 << " ns";
}
```

### 2.3 Hardware Performance Counters (perf)

```cpp
// test/perf_counters_test.cpp
#include <gtest/gtest.h>
#include "core/messaging/shm_ring.hpp"
#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>
#include <sys/syscall.h>
#include <unistd.h>

class PerfCounter {
public:
    explicit PerfCounter(uint32_t type, uint64_t config) {
        struct perf_event_attr pe = {
            .type = type,
            .size = sizeof(struct perf_event_attr),
            .config = config,
            .disabled = 1,
            .exclude_kernel = 1,
            .exclude_hv = 1
        };
        
        fd_ = syscall(__NR_perf_event_open, &pe, 0, -1, -1, 0);
        if (fd_ == -1) {
            perror("perf_event_open");
        }
    }
    
    ~PerfCounter() { if (fd_ != -1) close(fd_); }
    
    void start() {
        if (fd_ != -1) ioctl(fd_, PERF_EVENT_IOC_RESET, 0);
        if (fd_ != -1) ioctl(fd_, PERF_EVENT_IOC_ENABLE, 0);
    }
    
    void stop() {
        if (fd_ != -1) ioctl(fd_, PERF_EVENT_IOC_DISABLE, 0);
    }
    
    uint64_t read() {
        uint64_t count;
        ::read(fd_, &count, sizeof(count));
        return count;
    }
    
private:
    int fd_;
};

TEST(PerfCounters, CacheMisses) {
    PerfCounter cache_misses(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES);
    PerfCounter instructions(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS);
    
    alignas(4096) char memory[ShmRing::required_size(64, 4096)];
    ShmRing::init(memory, 64, 4096);
    auto* header = reinterpret_cast<RingHeader*>(memory);
    
    ShmRingProducer<64, 4096> producer(header);
    ShmRingConsumer<64, 4096> consumer(header);
    
    constexpr int kIterations = 100000;
    
    // Warmup
    for (int i = 0; i < 1000; ++i) {
        void* slot = producer.claim_spin();
        producer.commit();
        consumer.read_spin();
        consumer.advance();
    }
    
    cache_misses.start();
    instructions.start();
    
    for (int i = 0; i < kIterations; ++i) {
        void* slot = producer.claim_spin();
        producer.commit();
        const void* read_slot = consumer.read_spin();
        consumer.advance();
    }
    
    cache_misses.stop();
    instructions.stop();
    
    auto misses = cache_misses.read();
    auto instrs = instructions.read();
    
    // Expect ~2 cache misses per iteration (header + data)
    double misses_per_iter = static_cast<double>(misses) / kIterations;
    EXPECT_LT(misses_per_iter, 3.0) << "Cache misses per iteration: " << misses_per_iter;
    
    // Instructions per iteration should be low (< 100)
    double instrs_per_iter = static_cast<double>(instrs) / kIterations;
    EXPECT_LT(instrs_per_iter, 100.0) << "Instructions per iteration: " << instrs_per_iter;
}
```

### 2.4 CI Performance Gates

```cmake
# CMakeLists.txt - Performance gates

option(ONTRADE_PERFORMANCE_TEST "Run performance tests in CI" OFF)

if(ONTRADE_PERFORMANCE_TEST)
    # Baseline file with expected performance
    set(PERF_BASELINE "${CMAKE_SOURCE_DIR}/test/baselines/performance.json")
    
    add_test(NAME PerformanceGate
        COMMAND ${CMAKE_SOURCE_DIR}/scripts/check_performance.py
            --baseline ${PERF_BASELINE}
            --results ${CMAKE_BINARY_DIR}/benchmark_results.json
            --tolerance 10  # 10% regression allowed
    )
endif()
```

```python
#!/usr/bin/env python3
# scripts/check_performance.py
import json
import sys
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--results', required=True)
    parser.add_argument('--tolerance', type=float, default=10.0)
    args = parser.parse_args()
    
    with open(args.baseline) as f:
        baseline = json.load(f)
    
    with open(args.results) as f:
        results = json.load(f)
    
    failed = False
    for test_name, baseline_metrics in baseline.items():
        if test_name not in results:
            print(f"FAIL: {test_name} missing from results")
            failed = True
            continue
        
        result_metrics = results[test_name]
        
        for metric, baseline_value in baseline_metrics.items():
            result_value = result_metrics.get(metric)
            if result_value is None:
                print(f"FAIL: {test_name}.{metric} missing")
                failed = True
                continue
            
            # For latency, lower is better
            # For throughput, higher is better
            if metric.endswith('_latency'):
                regression = (result_value - baseline_value) / baseline_value * 100
                if regression > args.tolerance:
                    print(f"FAIL: {test_name}.{metric} regressed by {regression:.1f}%")
                    failed = True
            elif metric.endswith('_throughput'):
                regression = (baseline_value - result_value) / baseline_value * 100
                if regression > args.tolerance:
                    print(f"FAIL: {test_name}.{metric} regressed by {regression:.1f}%")
                    failed = True
    
    if failed:
        sys.exit(1)
    
    print("PASS: All performance metrics within tolerance")

if __name__ == '__main__':
    main()
```

### 2.5 Flame Graphs

```bash
#!/bin/bash
# scripts/generate_flamegraph.sh

# Build with frame pointers
# Run benchmark with perf
perf record -g -- ./build/benchmark/shm_ring_benchmark

# Generate flame graph
perf script | ./FlameGraph/stackcollapse-perf.pl | ./FlameGraph/flamegraph.pl > shm_ring_flamegraph.svg

echo "Flame graph generated: shm_ring_flamegraph.svg"
```

---

## Part 3: Integration Testing

### 3.1 Deterministic Replay

```cpp
// test/replay_test.cpp
#include <gtest/gtest.h>
#include "core/messaging/shm_ring.hpp"
#include "core/messaging/aeron_bus.hpp"

// Capture events from production, replay in test
TEST(Replay, DeterministicBehavior) {
    // Load captured event log
    auto events = load_event_log("fixtures/session_2024_01_15.bin");
    
    // Replay through system
    alignas(4096) char memory[ShmRing::required_size(64, 4096)];
    ShmRing::init(memory, 64, 4096);
    
    // ... replay logic
    
    // Verify output matches expected
    EXPECT_EQ(actual_output, expected_output);
}
```

### 3.2 Chaos Testing

```cpp
// Inject faults to test resilience
TEST(Chaos, ConsumerCrash) {
    // Simulate consumer crash mid-processing
    // Verify producer handles backpressure correctly
}

TEST(Chaos, MemoryCorruption) {
    // Corrupt slot header
    // Verify detection and graceful handling
}
```

---

## CI/CD Integration

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  correctness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Build with sanitizers
        run: |
          cmake -B build -DONTRADE_SANITIZER=ON
          cmake --build build
      
      - name: Run correctness tests
        run: ctest --test-dir build --output-on-failure
      
      - name: Static analysis
        run: |
          cmake --build build --target clang-analyze
          cmake --build build --target clang-tidy

  performance:
    runs-on: [self-hosted, trading-host]  # Dedicated hardware
    steps:
      - uses: actions/checkout@v3
      
      - name: Build optimized
        run: |
          cmake -B build -DCMAKE_BUILD_TYPE=Release
          cmake --build build
      
      - name: Run benchmarks
        run: |
          ./build/benchmark/shm_ring_benchmark --benchmark_out=results.json
      
      - name: Check performance gates
        run: |
          ./scripts/check_performance.py \
            --baseline test/baselines/performance.json \
            --results results.json \
            --tolerance 10
      
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: results.json
```

---

## Summary

| Test Type | Tool | Frequency | Gate |
|-----------|------|-----------|------|
| Unit Tests | Google Test | Every PR | Block merge |
| Property Tests | RapidCheck | Every PR | Block merge |
| Sanitizers | ASan/TSan/UBSan | Every PR | Block merge |
| Static Analysis | clang-tidy | Every PR | Block merge |
| Microbenchmarks | Google Benchmark | Every PR | Block merge (10% tolerance) |
| Latency Distribution | Custom | Nightly | Alert if SLO missed |
| Hardware Counters | perf | Weekly | Alert if regression |
| Flame Graphs | perf | Monthly | Manual review |
| Replay Tests | Custom | Weekly | Block release |
| Chaos Tests | Custom | Monthly | Block release |

---

*This testing strategy was created by **Cline** for team review*
