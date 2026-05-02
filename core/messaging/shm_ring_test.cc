#include "core/messaging/shm_ring.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

namespace ontrade::messaging {
namespace {

using TestRing = SpscRing<64, 8>;

class SpscRingTest : public ::testing::Test {
protected:
    alignas(detail::kCacheLine) std::byte buf_[TestRing::kStorageBytes]{};
    TestRing ring_{TestRing::create(buf_)};
};

TEST_F(SpscRingTest, EmptyRingReadsNullptr) {
    EXPECT_EQ(ring_.try_read(), nullptr);
    EXPECT_EQ(ring_.pending(), 0u);
}

TEST_F(SpscRingTest, ClaimWriteCommitReadRelease) {
    auto* slot = ring_.try_claim();
    ASSERT_NE(slot, nullptr);
    const std::uint32_t payload = 0xdeadbeefu;
    std::memcpy(slot, &payload, sizeof(payload));
    ring_.commit();

    EXPECT_EQ(ring_.pending(), 1u);

    const auto* rslot = ring_.try_read();
    ASSERT_NE(rslot, nullptr);
    std::uint32_t out = 0;
    std::memcpy(&out, rslot, sizeof(out));
    EXPECT_EQ(out, payload);
    ring_.release();

    EXPECT_EQ(ring_.pending(), 0u);
    EXPECT_EQ(ring_.try_read(), nullptr);
}

TEST_F(SpscRingTest, FullRingClaimsReturnNullptr) {
    for (std::size_t i = 0; i < TestRing::kSlotCount; ++i) {
        auto* slot = ring_.try_claim();
        ASSERT_NE(slot, nullptr) << "claim " << i;
        ring_.commit();
    }
    EXPECT_EQ(ring_.try_claim(), nullptr) << "ring should be full";

    const auto* rslot = ring_.try_read();
    ASSERT_NE(rslot, nullptr);
    ring_.release();
    EXPECT_NE(ring_.try_claim(), nullptr);
}

TEST_F(SpscRingTest, AttachSeesCreatorState) {
    auto* slot = ring_.try_claim();
    ASSERT_NE(slot, nullptr);
    const std::uint32_t payload = 0xfeedfaceu;
    std::memcpy(slot, &payload, sizeof(payload));
    ring_.commit();

    auto attached = TestRing::attach(buf_);
    const auto* rslot = attached.try_read();
    ASSERT_NE(rslot, nullptr);
    std::uint32_t out = 0;
    std::memcpy(&out, rslot, sizeof(out));
    EXPECT_EQ(out, payload);
}

TEST(SpscRingThreaded, FifoOrderingAcrossThreads) {
    using Ring = SpscRing<sizeof(std::uint32_t), 1024>;
    alignas(detail::kCacheLine) static std::byte storage[Ring::kStorageBytes]{};
    auto ring = Ring::create(storage);

    constexpr std::uint32_t kCount = 100'000;
    std::atomic<bool> consumer_failed{false};
    std::vector<std::uint32_t> received;
    received.reserve(kCount);

    std::thread consumer([&] {
        std::uint32_t expected = 0;
        while (received.size() < kCount) {
            const auto* slot = ring.try_read();
            if (!slot) {
                std::this_thread::yield();
                continue;
            }
            std::uint32_t got = 0;
            std::memcpy(&got, slot, sizeof(got));
            if (got != expected) {
                consumer_failed.store(true, std::memory_order_release);
                return;
            }
            received.push_back(got);
            ++expected;
            ring.release();
        }
    });

    for (std::uint32_t i = 0; i < kCount; ++i) {
        std::byte* slot;
        while ((slot = ring.try_claim()) == nullptr) {
            std::this_thread::yield();
        }
        std::memcpy(slot, &i, sizeof(i));
        ring.commit();
    }
    consumer.join();
    EXPECT_FALSE(consumer_failed.load(std::memory_order_acquire));
    EXPECT_EQ(received.size(), kCount);
}

}  // namespace
}  // namespace ontrade::messaging
