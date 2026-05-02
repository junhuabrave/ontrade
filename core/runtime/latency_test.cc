#include "core/runtime/latency.hpp"

#include <gtest/gtest.h>

#include "core/runtime/clock.hpp"

namespace ontrade::runtime {
namespace {

TEST(StampHop, WritesToCorrectField) {
    MockClock clk;
    clk.set_wall(1'000'000);

    proto::hot::Timestamps ts{};

    stamp_hop(ts, HopStamp::Origin, clk);
    EXPECT_EQ(ts.origin_ns, 1'000'000);
    EXPECT_EQ(ts.ingress_ns, 0);

    clk.set_wall(2'000'000);
    stamp_hop(ts, HopStamp::Ingress, clk);
    EXPECT_EQ(ts.ingress_ns, 2'000'000);

    clk.set_wall(3'000'000);
    stamp_hop(ts, HopStamp::Decision, clk);
    EXPECT_EQ(ts.decision_ns, 3'000'000);

    clk.set_wall(4'000'000);
    stamp_hop(ts, HopStamp::Submit, clk);
    EXPECT_EQ(ts.submit_ns, 4'000'000);
}

TEST(HopDelta, ReturnsDifferenceWhenBothStamped) {
    MockClock clk;
    proto::hot::Timestamps ts{};

    clk.set_wall(1'000'000);
    stamp_hop(ts, HopStamp::Ingress, clk);
    clk.set_wall(1'000'250);
    stamp_hop(ts, HopStamp::Decision, clk);

    EXPECT_EQ(hop_delta(ts, HopStamp::Ingress, HopStamp::Decision), 250);
}

TEST(HopDelta, ZeroWhenSourceUnstamped) {
    MockClock clk;
    proto::hot::Timestamps ts{};
    clk.set_wall(5'000);
    stamp_hop(ts, HopStamp::Decision, clk);
    // Ingress never stamped -> zero, must not pollute the delta.
    EXPECT_EQ(hop_delta(ts, HopStamp::Ingress, HopStamp::Decision), 0);
}

TEST(LatencyHistogram, BucketBoundariesArePowersOfTwo) {
    using LH = LatencyHistogram;
    EXPECT_EQ(LH::bucket_lower_bound_ns(0), 0);
    EXPECT_EQ(LH::bucket_lower_bound_ns(1), 64);     // 2^6
    EXPECT_EQ(LH::bucket_lower_bound_ns(2), 128);    // 2^7
    EXPECT_EQ(LH::bucket_lower_bound_ns(3), 256);    // 2^8
}

TEST(LatencyHistogram, BucketAssignment) {
    using LH = LatencyHistogram;
    EXPECT_EQ(LH::bucket_for(0), 0U);
    EXPECT_EQ(LH::bucket_for(63), 0U);    // below floor
    EXPECT_EQ(LH::bucket_for(64), 1U);    // 2^6 -> bucket 1
    EXPECT_EQ(LH::bucket_for(127), 1U);
    EXPECT_EQ(LH::bucket_for(128), 2U);   // 2^7 -> bucket 2
    EXPECT_EQ(LH::bucket_for(1024), 5U);  // 2^10 -> bucket 5
}

TEST(LatencyHistogram, OverflowGoesToTopBucket) {
    LatencyHistogram h;
    // ~10 seconds is well past the top bucket.
    h.record(10'000'000'000);
    EXPECT_EQ(h.bucket_count(LatencyHistogram::kBucketCount - 1), 1U);
    EXPECT_EQ(h.total_count(), 1U);
}

TEST(LatencyHistogram, RecordCountsAccumulate) {
    LatencyHistogram h;
    for (int i = 0; i < 100; ++i) {
        h.record(200);  // bucket 2 (128..256)
    }
    EXPECT_EQ(h.bucket_count(2), 100U);
    EXPECT_EQ(h.total_count(), 100U);
}

TEST(LatencyHistogram, MaxTracksLargestObservation) {
    LatencyHistogram h;
    h.record(500);
    h.record(50'000);
    h.record(10'000);
    EXPECT_EQ(h.max_ns(), 50'000);
}

TEST(LatencyHistogram, QuantileApproximatesPercentile) {
    LatencyHistogram h;
    // 100 samples around 200ns, 10 samples around 100µs.
    for (int i = 0; i < 100; ++i) h.record(200);
    for (int i = 0; i < 10; ++i) h.record(100'000);
    // p50 should be in the 200ns bucket.
    const auto p50 = h.quantile_ns(0.5);
    EXPECT_GE(p50, 64);
    EXPECT_LE(p50, 256);
    // p99 should be up at the 100µs region.
    const auto p99 = h.quantile_ns(0.99);
    EXPECT_GT(p99, 1'000);
}

TEST(LatencyHistogram, ResetClearsAllState) {
    LatencyHistogram h;
    h.record(500);
    h.record(50'000);
    h.reset();
    EXPECT_EQ(h.total_count(), 0U);
    EXPECT_EQ(h.max_ns(), 0);
    EXPECT_EQ(h.bucket_count(2), 0U);
}

}  // namespace
}  // namespace ontrade::runtime
