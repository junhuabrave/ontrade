#include "core/runtime/clock.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace ontrade::runtime {
namespace {

TEST(SystemClockTest, MonotonicNowMovesForward) {
    SystemClock c;
    const auto t1 = c.now_ns();
    std::this_thread::sleep_for(std::chrono::microseconds(10));
    const auto t2 = c.now_ns();
    EXPECT_GT(t2, t1);
}

TEST(SystemClockTest, WallTimeIsRecent) {
    SystemClock c;
    const auto wall = c.wall_ns();
    constexpr nanos_t k2025 = 1735689600LL * 1'000'000'000LL;
    constexpr nanos_t k2040 = 2208988800LL * 1'000'000'000LL;
    EXPECT_GT(wall, k2025);
    EXPECT_LT(wall, k2040);
}

TEST(MockClockTest, AdvanceMovesNowForward) {
    MockClock c;
    EXPECT_EQ(c.now_ns(), 0);
    c.advance(100);
    EXPECT_EQ(c.now_ns(), 100);
    c.advance(50);
    EXPECT_EQ(c.now_ns(), 150);
}

TEST(MockClockTest, WallIsIndependentOfMonotonic) {
    MockClock c;
    c.set_wall(42);
    EXPECT_EQ(c.wall_ns(), 42);
    c.advance(1'000);
    EXPECT_EQ(c.wall_ns(), 42);
}

TEST(MockClockTest, SetNowOverrides) {
    MockClock c;
    c.set_now(1'000'000);
    EXPECT_EQ(c.now_ns(), 1'000'000);
    c.advance(5);
    EXPECT_EQ(c.now_ns(), 1'000'005);
}

template <ClockLike C>
nanos_t deltaTwoReads(C& c) {
    const auto t0 = c.now_ns();
    const auto t1 = c.now_ns();
    return t1 - t0;
}

TEST(ClockConcept, GenericFunctionAcceptsBoth) {
    SystemClock sc;
    MockClock mc;
    EXPECT_GE(deltaTwoReads(sc), 0);
    EXPECT_EQ(deltaTwoReads(mc), 0);
}

}  // namespace
}  // namespace ontrade::runtime
