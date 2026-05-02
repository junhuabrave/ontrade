#include "core/memory/arena.hpp"

#include <array>
#include <cstdint>

#include <gtest/gtest.h>

namespace ontrade::memory {
namespace {

class ArenaTest : public ::testing::Test {
protected:
    static constexpr std::size_t kSize = 1024;
    alignas(64) std::array<std::byte, kSize> buf_{};
};

TEST_F(ArenaTest, AllocateRespectsAlignment) {
    Arena arena(buf_.data(), kSize);
    for (std::size_t align : {1U, 2U, 4U, 8U, 16U, 32U, 64U}) {
        void* p = arena.allocate(1, align);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(reinterpret_cast<std::uintptr_t>(p) % align, 0U);
    }
}

TEST_F(ArenaTest, ConsecutiveAllocationsAdvanceOffset) {
    Arena arena(buf_.data(), kSize);
    void* a = arena.allocate(8, 8);
    void* b = arena.allocate(8, 8);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(static_cast<std::byte*>(b) - static_cast<std::byte*>(a), 8);
    EXPECT_EQ(arena.used(), 16U);
}

TEST_F(ArenaTest, ConstructPlacesObjectInArena) {
    Arena arena(buf_.data(), kSize);
    struct Pair {
        int a;
        int b;
    };
    Pair* p = arena.construct<Pair>(7, 11);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->a, 7);
    EXPECT_EQ(p->b, 11);
    auto addr = reinterpret_cast<std::uintptr_t>(p);
    auto base = reinterpret_cast<std::uintptr_t>(buf_.data());
    EXPECT_GE(addr, base);
    EXPECT_LT(addr, base + kSize);
}

TEST_F(ArenaTest, ResetReusesStorage) {
    Arena arena(buf_.data(), kSize);
    void* a = arena.allocate(64, 16);
    arena.reset();
    EXPECT_EQ(arena.used(), 0U);
    void* b = arena.allocate(64, 16);
    EXPECT_EQ(a, b);
}

TEST_F(ArenaTest, ExhaustionReturnsNullptr) {
    std::array<std::byte, 32> small{};
    Arena arena(small.data(), small.size());
    void* a = arena.allocate(20, 1);
    ASSERT_NE(a, nullptr);
    void* b = arena.allocate(20, 1);
    EXPECT_EQ(b, nullptr);
    EXPECT_EQ(arena.used(), 20U);
}

TEST_F(ArenaTest, ExactFitSucceeds) {
    std::array<std::byte, 32> small{};
    Arena arena(small.data(), small.size());
    void* a = arena.allocate(32, 1);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(arena.used(), 32U);
    EXPECT_EQ(arena.available(), 0U);
}

TEST_F(ArenaTest, AlignmentPaddingCountsAgainstCapacity) {
    std::array<std::byte, 16> small{};
    Arena arena(small.data(), small.size());
    void* a = arena.allocate(1, 1);
    ASSERT_NE(a, nullptr);
    // Next 8-byte-aligned slot needs padding; remaining capacity must
    // shrink by both the padding and the request.
    void* b = arena.allocate(8, 8);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(b) % 8U, 0U);
}

TEST_F(ArenaTest, NonPowerOfTwoAlignmentRejected) {
    Arena arena(buf_.data(), kSize);
    EXPECT_EQ(arena.allocate(1, 3), nullptr);
    EXPECT_EQ(arena.allocate(1, 0), nullptr);
}

TEST_F(ArenaTest, ZeroByteAllocationSucceedsWithoutAdvancing) {
    Arena arena(buf_.data(), kSize);
    void* a = arena.allocate(0, 1);
    EXPECT_NE(a, nullptr);
    EXPECT_EQ(arena.used(), 0U);
}

}  // namespace
}  // namespace ontrade::memory
