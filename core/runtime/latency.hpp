#pragma once

// Hot-path observability primitives.
//
// The schema in core/proto/hot/messages.hpp reserves four ns timestamps per
// order (origin/ingress/decision/submit). Every component on the order path
// stamps the field corresponding to "when I produced this output" — that is
// the entire serialization protocol for latency tracing. Reconstructing
// per-hop latency is then a subtraction.
//
// This header gives:
//   - a typed function for stamping a Timestamps field consistently
//     (always wall_ns, always via the runtime clock so replay is mockable);
//   - a fixed-bucket histogram with a power-of-two-spaced layout from 100ns
//     to ~1s, suitable for online aggregation per shm-hop with no allocation
//     and lock-free single-writer access.
//
// The histogram is intentionally lossy at the high end (anything > 1s drops
// into an overflow bucket) because trading-host hops past one second are
// pathological and don't need fine resolution; what we care about is sub-µs
// to low-ms with good fidelity.
//
// Aggregation across threads/processes happens off the hot path: a reader
// snapshots the histogram, merges into a global view, and ships to the
// metrics backend. The hot-path writer never blocks.

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "core/proto/hot/messages.hpp"
#include "core/runtime/clock.hpp"

namespace ontrade::runtime {

// Which timestamp field to stamp on an order message. Mirrors the field set
// in proto::hot::Timestamps so the call site reads naturally.
enum class HopStamp : std::uint8_t {
    Origin = 0,
    Ingress = 1,
    Decision = 2,
    Submit = 3,
};

// Stamp the given hop on a hot-path Timestamps struct using the supplied
// clock. This is the only intended path to populate Timestamps so all
// stamps share the same clock source and can be diff'd correctly.
template <ClockLike Clock>
inline void stamp_hop(proto::hot::Timestamps& ts, HopStamp hop, const Clock& clk) noexcept {
    const auto t = clk.wall_ns();
    switch (hop) {
        case HopStamp::Origin:   ts.origin_ns = t; break;
        case HopStamp::Ingress:  ts.ingress_ns = t; break;
        case HopStamp::Decision: ts.decision_ns = t; break;
        case HopStamp::Submit:   ts.submit_ns = t; break;
    }
}

// Read a stamped field. Returns 0 if the field hasn't been stamped yet
// (zero-init means "not stamped" by convention; producers must stamp every
// field they own).
[[nodiscard]] inline nanos_t read_hop(const proto::hot::Timestamps& ts, HopStamp hop) noexcept {
    switch (hop) {
        case HopStamp::Origin:   return ts.origin_ns;
        case HopStamp::Ingress:  return ts.ingress_ns;
        case HopStamp::Decision: return ts.decision_ns;
        case HopStamp::Submit:   return ts.submit_ns;
    }
    return 0;
}

// Hop-to-hop delta. Returns 0 if either side is unstamped, so an unstamped
// hop never contaminates aggregated stats.
[[nodiscard]] inline nanos_t hop_delta(const proto::hot::Timestamps& ts,
                                       HopStamp from,
                                       HopStamp to) noexcept {
    const auto a = read_hop(ts, from);
    const auto b = read_hop(ts, to);
    if (a == 0 || b == 0) return 0;
    return b - a;
}

// Power-of-two-spaced histogram. Bucket i covers [2^(i+kFloorLog2), 2^(i+1+kFloorLog2)) ns,
// with bucket 0 acting as the "<= floor" sink and the last bucket as the overflow sink.
// Range floor is 64ns (2^6), top bucket overflow is 2^30 ≈ 1.07s. Total 25 buckets.
//
// Single-writer hot-path semantics: increments are relaxed atomic adds — one
// store-add, no contention because each producer thread owns its histogram.
// Readers snapshot with relaxed loads; eventual consistency is fine for
// metrics export. Two histograms are merged by adding bucket counts.
class LatencyHistogram {
public:
    static constexpr std::size_t kBucketCount = 25;
    static constexpr int kFloorLog2 = 6;  // 64ns
    static_assert(kBucketCount >= 2);

    LatencyHistogram() noexcept = default;

    LatencyHistogram(const LatencyHistogram&) = delete;
    LatencyHistogram& operator=(const LatencyHistogram&) = delete;

    void record(nanos_t ns) noexcept {
        const std::size_t b = bucket_for(ns);
        buckets_[b].fetch_add(1, std::memory_order_relaxed);
        // Maintain a relaxed running max for diagnostics; not a tight upper
        // bound under contention but we are single-writer per histogram.
        const auto cur = max_ns_.load(std::memory_order_relaxed);
        if (ns > cur) {
            max_ns_.store(ns, std::memory_order_relaxed);
        }
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    // Map a latency in ns to a bucket index. Bucket 0 catches everything
    // below the floor (2^kFloorLog2 = 64ns); bucket b > 0 covers
    // [2^(b+kFloorLog2-1), 2^(b+kFloorLog2)). Public for inspection in tests.
    [[nodiscard]] static std::size_t bucket_for(nanos_t ns) noexcept {
        if (ns <= 0) return 0;
        std::uint64_t v = static_cast<std::uint64_t>(ns);
        int log2 = 0;
        while (v > 1) { v >>= 1; ++log2; }
        if (log2 < kFloorLog2) return 0;
        const int idx = log2 - kFloorLog2 + 1;
        if (static_cast<std::size_t>(idx) >= kBucketCount) {
            return kBucketCount - 1;
        }
        return static_cast<std::size_t>(idx);
    }

    // The lower bound of bucket b (in ns). Bucket 0 is [0, 2^kFloorLog2);
    // bucket b > 0 is [2^(b+kFloorLog2-1), 2^(b+kFloorLog2)).
    [[nodiscard]] static nanos_t bucket_lower_bound_ns(std::size_t b) noexcept {
        if (b == 0) return 0;
        return static_cast<nanos_t>(1ULL << (b + kFloorLog2 - 1));
    }

    [[nodiscard]] std::uint64_t bucket_count(std::size_t b) const noexcept {
        return buckets_[b].load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::uint64_t total_count() const noexcept {
        return count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] nanos_t max_ns() const noexcept {
        return max_ns_.load(std::memory_order_relaxed);
    }

    // Approximate quantile by linear search across buckets. Returns the
    // lower bound of the bucket containing the q-th observation, or 0 if no
    // data. q is in [0, 1].
    [[nodiscard]] nanos_t quantile_ns(double q) const noexcept {
        const auto total = total_count();
        if (total == 0) return 0;
        if (q < 0.0) q = 0.0;
        if (q > 1.0) q = 1.0;
        const auto target = static_cast<std::uint64_t>(q * static_cast<double>(total) + 0.5);
        std::uint64_t cum = 0;
        for (std::size_t i = 0; i < kBucketCount; ++i) {
            cum += bucket_count(i);
            if (cum >= target) {
                return bucket_lower_bound_ns(i);
            }
        }
        return bucket_lower_bound_ns(kBucketCount - 1);
    }

    // Reset all counters. Off-hot-path — used by snapshot+reset in metrics
    // export when the consumer wants delta-since-last-export semantics.
    void reset() noexcept {
        for (auto& b : buckets_) b.store(0, std::memory_order_relaxed);
        count_.store(0, std::memory_order_relaxed);
        max_ns_.store(0, std::memory_order_relaxed);
    }

private:
    std::array<std::atomic<std::uint64_t>, kBucketCount> buckets_{};
    std::atomic<std::uint64_t> count_{0};
    std::atomic<nanos_t> max_ns_{0};
};

}  // namespace ontrade::runtime
