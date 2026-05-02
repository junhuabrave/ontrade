#include "core/memory/pool.hpp"

#include <array>
#include <cstddef>

#include <gtest/gtest.h>

namespace ontrade::memory {
namespace {

struct Counters {
    int constructs = 0;
    int destructs = 0;
};

struct Tracked {
    Counters* c;
    int value;
    Tracked(Counters* counters, int v) : c(counters), value(v) { ++c->constructs; }
    ~Tracked() { ++c->destructs; }
};

TEST(ObjectPool, CapacityReportsConfigured) {
    ObjectPool<int, 8> pool;
    EXPECT_EQ(pool.capacity(), 8U);
    EXPECT_EQ(pool.free_slots(), 8U);
    EXPECT_EQ(pool.in_use(), 0U);
}

TEST(ObjectPool, ConstructPreservesArgs) {
    Counters c{};
    ObjectPool<Tracked, 4> pool;
    Tracked* t = pool.construct(&c, 42);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->value, 42);
    EXPECT_EQ(c.constructs, 1);
    EXPECT_EQ(c.destructs, 0);
    EXPECT_EQ(pool.in_use(), 1U);
    pool.destroy(t);
    EXPECT_EQ(c.destructs, 1);
}

TEST(ObjectPool, ExhaustionReturnsNullptr) {
    ObjectPool<int, 3> pool;
    int* a = pool.construct(1);
    int* b = pool.construct(2);
    int* c = pool.construct(3);
    int* d = pool.construct(4);
    EXPECT_NE(a, nullptr);
    EXPECT_NE(b, nullptr);
    EXPECT_NE(c, nullptr);
    EXPECT_EQ(d, nullptr);
    EXPECT_EQ(pool.free_slots(), 0U);
    pool.destroy(a);
    pool.destroy(b);
    pool.destroy(c);
}

TEST(ObjectPool, DestroyRestoresCapacity) {
    ObjectPool<int, 2> pool;
    int* a = pool.construct(10);
    int* b = pool.construct(20);
    EXPECT_EQ(pool.free_slots(), 0U);
    pool.destroy(a);
    EXPECT_EQ(pool.free_slots(), 1U);
    pool.destroy(b);
    EXPECT_EQ(pool.free_slots(), 2U);
}

TEST(ObjectPool, DestroyNullptrIsNoop) {
    ObjectPool<int, 4> pool;
    pool.destroy(nullptr);
    EXPECT_EQ(pool.free_slots(), 4U);
}

TEST(ObjectPool, RecycledSlotsServeNewObjects) {
    ObjectPool<int, 2> pool;
    int* a = pool.construct(7);
    int* b = pool.construct(8);
    pool.destroy(a);
    int* c = pool.construct(99);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(*c, 99);
    pool.destroy(b);
    pool.destroy(c);
}

TEST(ObjectPool, DestructorRunsOnDestroy) {
    Counters c{};
    {
        ObjectPool<Tracked, 4> pool;
        Tracked* t1 = pool.construct(&c, 1);
        Tracked* t2 = pool.construct(&c, 2);
        pool.destroy(t1);
        pool.destroy(t2);
    }
    EXPECT_EQ(c.constructs, 2);
    EXPECT_EQ(c.destructs, 2);
}

TEST(ObjectPool, LeakedObjectsAreNotDestructedAtPoolDestruction) {
    // Pool does not track in-use objects' destructors — caller is responsible
    // for destroy(). This test pins that behavior so callers don't assume
    // RAII cleanup of objects they failed to release.
    Counters c{};
    {
        ObjectPool<Tracked, 4> pool;
        (void)pool.construct(&c, 1);
    }
    EXPECT_EQ(c.constructs, 1);
    EXPECT_EQ(c.destructs, 0);
}

}  // namespace
}  // namespace ontrade::memory
