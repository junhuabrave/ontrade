#pragma once

// Time is the foundation of every replayable system. All hot-path components
// take a Clock by template parameter (or const reference for non-hot paths)
// so backtests, replays, and unit tests drive time deterministically. Do not
// call std::chrono::*_clock::now() directly anywhere outside this header.
//
//   now_ns()  : monotonic nanoseconds. Used for latency measurement and
//               timeouts. Never moves backward.
//   wall_ns() : nanoseconds since UNIX epoch. Used for stamping records.
//               In production this is hardware-PTP backed; under NTP it can
//               jump. In mocks it is whatever the test sets.

#include <atomic>
#include <chrono>
#include <concepts>
#include <cstdint>

namespace ontrade::runtime {

using nanos_t = std::int64_t;

class SystemClock {
public:
    [[nodiscard]] nanos_t now_ns() const noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    [[nodiscard]] nanos_t wall_ns() const noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }
};

class MockClock {
public:
    [[nodiscard]] nanos_t now_ns() const noexcept {
        return now_ns_.load(std::memory_order_acquire);
    }
    [[nodiscard]] nanos_t wall_ns() const noexcept {
        return wall_ns_.load(std::memory_order_acquire);
    }

    void advance(nanos_t ns) noexcept {
        now_ns_.fetch_add(ns, std::memory_order_release);
    }
    void set_now(nanos_t ns) noexcept {
        now_ns_.store(ns, std::memory_order_release);
    }
    void set_wall(nanos_t ns) noexcept {
        wall_ns_.store(ns, std::memory_order_release);
    }

private:
    std::atomic<nanos_t> now_ns_{0};
    std::atomic<nanos_t> wall_ns_{0};
};

template <typename C>
concept ClockLike = requires(const C& c) {
    { c.now_ns() } -> std::same_as<nanos_t>;
    { c.wall_ns() } -> std::same_as<nanos_t>;
};

static_assert(ClockLike<SystemClock>);
static_assert(ClockLike<MockClock>);

}  // namespace ontrade::runtime
