#include "core/proto/hot/messages.hpp"

#include <cstring>
#include <type_traits>

#include <gtest/gtest.h>

namespace ontrade::proto::hot {
namespace {

// All hot-path messages must be trivially copyable so producers can memcpy
// into a shm slot and consumers can reinterpret_cast the slot bytes.
static_assert(std::is_trivially_copyable_v<OrderNew>);
static_assert(std::is_trivially_copyable_v<OrderCancel>);
static_assert(std::is_trivially_copyable_v<OrderReplace>);
static_assert(std::is_trivially_copyable_v<OrderAck>);
static_assert(std::is_trivially_copyable_v<OrderReject>);
static_assert(std::is_trivially_copyable_v<OrderFill>);
static_assert(std::is_trivially_copyable_v<OrderCancelAck>);
static_assert(std::is_trivially_copyable_v<BookUpdate>);
static_assert(std::is_trivially_copyable_v<TradeTick>);

static_assert(std::is_standard_layout_v<OrderNew>);
static_assert(std::is_standard_layout_v<OrderFill>);
static_assert(std::is_standard_layout_v<BookUpdate>);
static_assert(std::is_standard_layout_v<TradeTick>);

TEST(HotSchema, VersionConstants) {
    EXPECT_EQ(kSchemaMajor, 1);
    EXPECT_EQ(kSchemaMinor, 0);
}

TEST(HotSchema, OrderNewRoundTripViaMemcpy) {
    OrderNew src{};
    src.hdr.schema_major = kSchemaMajor;
    src.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderNew);
    src.hdr.seq = 42;
    src.ids.cl_ord_id = 0x0000'0001'0000'00ABULL;
    src.ids.instrument_id = 7;
    src.ids.venue_id = 3;
    src.qty_raw = 100;
    src.price_raw_e8 = 100'00'000'000;  // $100.00
    src.side = Side::Buy;
    src.ord_type = OrdType::Limit;
    src.tif = TimeInForce::IOC;
    src.ts.origin_ns = 1'000'000;
    src.ts.submit_ns = 2'000'000;

    alignas(OrderNew) std::byte wire[sizeof(OrderNew)]{};
    std::memcpy(wire, &src, sizeof(OrderNew));

    OrderNew dst{};
    std::memcpy(&dst, wire, sizeof(OrderNew));

    EXPECT_EQ(dst.hdr.schema_major, kSchemaMajor);
    EXPECT_EQ(dst.hdr.msg_type, static_cast<std::uint16_t>(MsgType::OrderNew));
    EXPECT_EQ(dst.hdr.seq, 42U);
    EXPECT_EQ(dst.ids.cl_ord_id, 0x0000'0001'0000'00ABULL);
    EXPECT_EQ(dst.ids.instrument_id, 7U);
    EXPECT_EQ(dst.qty_raw, 100);
    EXPECT_EQ(dst.price_raw_e8, 100'00'000'000);
    EXPECT_EQ(dst.side, Side::Buy);
    EXPECT_EQ(dst.ord_type, OrdType::Limit);
    EXPECT_EQ(dst.tif, TimeInForce::IOC);
    EXPECT_EQ(dst.ts.origin_ns, 1'000'000);
    EXPECT_EQ(dst.ts.submit_ns, 2'000'000);
}

TEST(HotSchema, FillRoundTripPreservesNegativeFees) {
    OrderFill src{};
    src.hdr.schema_major = kSchemaMajor;
    src.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderFill);
    src.fill_qty_raw = 50;
    src.fill_price_raw_e8 = 99'95'000'000;
    src.fee_raw_e8 = -12'500;  // rebate
    src.liquidity = LiquidityFlag::Maker;

    alignas(OrderFill) std::byte wire[sizeof(OrderFill)]{};
    std::memcpy(wire, &src, sizeof(OrderFill));

    OrderFill dst{};
    std::memcpy(&dst, wire, sizeof(OrderFill));

    EXPECT_EQ(dst.fill_qty_raw, 50);
    EXPECT_EQ(dst.fill_price_raw_e8, 99'95'000'000);
    EXPECT_EQ(dst.fee_raw_e8, -12'500);
    EXPECT_EQ(dst.liquidity, LiquidityFlag::Maker);
}

TEST(HotSchema, MsgTypeDiscriminatorReadFromHeaderBytes) {
    OrderCancel src{};
    src.hdr.schema_major = kSchemaMajor;
    src.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancel);
    src.hdr.seq = 9;

    alignas(OrderCancel) std::byte wire[sizeof(OrderCancel)]{};
    std::memcpy(wire, &src, sizeof(OrderCancel));

    // A receiver that only knows the wire bytes can dispatch on the
    // first 4 bytes of the header alone.
    Header hdr_view{};
    std::memcpy(&hdr_view, wire, sizeof(Header));
    EXPECT_EQ(hdr_view.schema_major, kSchemaMajor);
    EXPECT_EQ(hdr_view.msg_type, static_cast<std::uint16_t>(MsgType::OrderCancel));
}

TEST(HotSchema, AllMessagesFitInTypicalRingSlot) {
    // A 256-byte slot is the planned hot-ring slot size; pin that no
    // message has grown past it without conscious review.
    constexpr std::size_t kSlot = 256;
    EXPECT_LE(sizeof(OrderNew), kSlot);
    EXPECT_LE(sizeof(OrderCancel), kSlot);
    EXPECT_LE(sizeof(OrderReplace), kSlot);
    EXPECT_LE(sizeof(OrderAck), kSlot);
    EXPECT_LE(sizeof(OrderReject), kSlot);
    EXPECT_LE(sizeof(OrderFill), kSlot);
    EXPECT_LE(sizeof(OrderCancelAck), kSlot);
}

}  // namespace
}  // namespace ontrade::proto::hot
