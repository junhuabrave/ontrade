#include "core/gateways/loopback.hpp"

#include <array>
#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "core/messaging/shm_ring.hpp"
#include "core/oms/oms.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/risk/limits.hpp"
#include "core/runtime/clock.hpp"

namespace ontrade::gateways {
namespace {

using proto::hot::MsgType;
using proto::hot::OrderAck;
using proto::hot::OrderCancel;
using proto::hot::OrderCancelAck;
using proto::hot::OrderFill;
using proto::hot::OrderNew;
using proto::hot::Side;

constexpr std::size_t kSlots = 16;
constexpr std::size_t kSlotSize = proto::hot::kHotSlotBytes;

// ---------- Loopback gateway in isolation ----------

class LoopbackFixture : public ::testing::Test {
protected:
    using FromOmsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using ToOmsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using GatewayType = LoopbackGateway<kSlots, kSlots, runtime::MockClock>;

    void SetUp() override {
        clk_.set_wall(1'000'000);
        from_oms_ = FromOmsRing::create(from_oms_storage_.data());
        to_oms_ = ToOmsRing::create(to_oms_storage_.data());
        gw_ = std::make_unique<GatewayType>(from_oms_, to_oms_, clk_);
    }

    void push_order_new(std::uint64_t cl_ord_id, std::int64_t qty,
                       std::int64_t price_e8, Side side = Side::Buy) {
        OrderNew o{};
        o.hdr.schema_major = proto::hot::kSchemaMajor;
        o.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderNew);
        o.ids.cl_ord_id = cl_ord_id;
        o.qty_raw = qty;
        o.price_raw_e8 = price_e8;
        o.side = side;
        auto* slot = from_oms_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &o, sizeof(o));
        from_oms_.commit();
    }

    void push_order_cancel(std::uint64_t cl_ord_id, std::uint64_t exch_ord_id) {
        OrderCancel c{};
        c.hdr.schema_major = proto::hot::kSchemaMajor;
        c.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancel);
        c.ids.cl_ord_id = cl_ord_id;
        c.ids.exch_ord_id = exch_ord_id;
        auto* slot = from_oms_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &c, sizeof(c));
        from_oms_.commit();
    }

    template <typename Msg>
    Msg read_to_oms() {
        const auto* slot = to_oms_.try_read();
        EXPECT_NE(slot, nullptr);
        Msg m{};
        std::memcpy(&m, slot, sizeof(m));
        to_oms_.release();
        return m;
    }

    [[nodiscard]] proto::hot::Header peek_header() {
        const auto* slot = to_oms_.try_read();
        if (slot == nullptr) return {};
        proto::hot::Header h{};
        std::memcpy(&h, slot, sizeof(h));
        return h;
    }

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, FromOmsRing::kStorageBytes> from_oms_storage_{};
    alignas(64) std::array<std::byte, ToOmsRing::kStorageBytes> to_oms_storage_{};
    FromOmsRing from_oms_{FromOmsRing::attach(from_oms_storage_.data())};
    ToOmsRing to_oms_{ToOmsRing::attach(to_oms_storage_.data())};
    std::unique_ptr<GatewayType> gw_;
};

TEST_F(LoopbackFixture, EmptyInputProcessesNothing) {
    EXPECT_EQ(gw_->poll(), 0U);
    EXPECT_EQ(to_oms_.try_read(), nullptr);
}

TEST_F(LoopbackFixture, OrderNewProducesAckThenFill) {
    push_order_new(/*cl_ord_id=*/42, /*qty=*/100, /*price=*/100'00'000'000);
    EXPECT_EQ(gw_->poll(), 1U);

    EXPECT_EQ(peek_header().msg_type, static_cast<std::uint16_t>(MsgType::OrderAck));
    auto ack = read_to_oms<OrderAck>();
    EXPECT_EQ(ack.ids.cl_ord_id, 42U);
    EXPECT_NE(ack.ids.exch_ord_id, 0U);

    auto fill = read_to_oms<OrderFill>();
    EXPECT_EQ(fill.ids.cl_ord_id, 42U);
    EXPECT_EQ(fill.ids.exch_ord_id, ack.ids.exch_ord_id);
    EXPECT_EQ(fill.fill_qty_raw, 100);
    EXPECT_EQ(fill.fill_price_raw_e8, 100'00'000'000);
}

TEST_F(LoopbackFixture, ExchOrdIdsAreMonotonic) {
    push_order_new(1, 10, 100'00'000'000);
    push_order_new(2, 10, 100'00'000'000);
    gw_->poll();

    auto a1 = read_to_oms<OrderAck>();
    (void)read_to_oms<OrderFill>();
    auto a2 = read_to_oms<OrderAck>();
    (void)read_to_oms<OrderFill>();

    EXPECT_GT(a2.ids.exch_ord_id, a1.ids.exch_ord_id);
}

TEST_F(LoopbackFixture, AutoAckDisabledSuppressesAck) {
    GatewayType::Behavior b;
    b.auto_ack = false;
    b.auto_fill = false;
    gw_->set_behavior(b);

    push_order_new(7, 10, 100'00'000'000);
    gw_->poll();

    EXPECT_EQ(to_oms_.try_read(), nullptr);
    EXPECT_EQ(gw_->orders_acked(), 0U);
    EXPECT_EQ(gw_->fills_emitted(), 0U);
}

TEST_F(LoopbackFixture, AutoFillDisabledLeavesOrderWorking) {
    GatewayType::Behavior b;
    b.auto_fill = false;
    gw_->set_behavior(b);

    push_order_new(7, 10, 100'00'000'000);
    gw_->poll();

    auto ack = read_to_oms<OrderAck>();
    EXPECT_EQ(ack.ids.cl_ord_id, 7U);
    EXPECT_EQ(to_oms_.try_read(), nullptr);
    EXPECT_EQ(gw_->fills_emitted(), 0U);
}

TEST_F(LoopbackFixture, OrderCancelProducesCancelAck) {
    push_order_cancel(/*cl_ord_id=*/9, /*exch_ord_id=*/0xABCD);
    gw_->poll();

    auto ca = read_to_oms<OrderCancelAck>();
    EXPECT_EQ(ca.ids.cl_ord_id, 9U);
    EXPECT_EQ(ca.ids.exch_ord_id, 0xABCDU);
    EXPECT_EQ(gw_->cancels_acked(), 1U);
}

TEST_F(LoopbackFixture, AutoCancelAckDisabledSuppressesAck) {
    GatewayType::Behavior b;
    b.auto_cancel_ack = false;
    gw_->set_behavior(b);

    push_order_cancel(9, 0xABCD);
    gw_->poll();

    EXPECT_EQ(to_oms_.try_read(), nullptr);
    EXPECT_EQ(gw_->cancels_acked(), 0U);
}

TEST_F(LoopbackFixture, ToOmsFullIncrementsDrops) {
    // Fill the to_oms ring first.
    for (std::size_t i = 0; i < kSlots; ++i) {
        auto* slot = to_oms_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memset(slot, 0, kSlotSize);
        to_oms_.commit();
    }
    EXPECT_EQ(to_oms_.try_claim(), nullptr);

    push_order_new(1, 10, 100'00'000'000);
    gw_->poll();

    // Both ack and fill should have failed → drops += 2.
    EXPECT_EQ(gw_->drops(), 2U);
}

TEST_F(LoopbackFixture, NonOrderMessagesAreIgnored) {
    // Push an OrderAck-shaped header (the gateway should never see acks
    // from the OMS — but if it does, drop it without crashing).
    proto::hot::Header h{};
    h.schema_major = proto::hot::kSchemaMajor;
    h.msg_type = static_cast<std::uint16_t>(MsgType::OrderAck);
    auto* slot = from_oms_.try_claim();
    ASSERT_NE(slot, nullptr);
    std::memset(slot, 0, kSlotSize);
    std::memcpy(slot, &h, sizeof(h));
    from_oms_.commit();

    EXPECT_EQ(gw_->poll(), 1U);
    EXPECT_EQ(to_oms_.try_read(), nullptr);
}

TEST_F(LoopbackFixture, GatewayStampsVenueAckIngressTime) {
    clk_.set_wall(5'555'555);
    push_order_new(1, 10, 100'00'000'000);
    clk_.set_wall(5'556'000);
    gw_->poll();

    auto ack = read_to_oms<OrderAck>();
    EXPECT_EQ(ack.ts.ingress_ns, 5'556'000);
}

// ---------- End-to-end: OMS ↔ LoopbackGateway ----------

class EndToEndFixture : public ::testing::Test {
protected:
    static constexpr std::size_t kRingSlots = 16;
    static constexpr std::size_t kMaxOpen = 8;

    using StrategyToOms = messaging::SpscRing<kSlotSize, kRingSlots>;
    using OmsToVenue = messaging::SpscRing<kSlotSize, kRingSlots>;
    using VenueToOms = messaging::SpscRing<kSlotSize, kRingSlots>;
    using OmsEvents = messaging::SpscRing<kSlotSize, kRingSlots>;

    using OmsType = oms::Oms<kRingSlots, kRingSlots, kRingSlots, kRingSlots,
                             kMaxOpen, runtime::MockClock>;
    using GatewayType = LoopbackGateway<kRingSlots, kRingSlots, runtime::MockClock>;

    void SetUp() override {
        clk_.set_wall(1'000'000);

        in_ = StrategyToOms::create(in_storage_.data());
        to_venue_ = OmsToVenue::create(to_venue_storage_.data());
        venue_in_ = VenueToOms::create(venue_in_storage_.data());
        events_ = OmsEvents::create(events_storage_.data());

        oms_ = std::make_unique<OmsType>(in_, venue_in_, to_venue_, events_, clk_);
        gw_ = std::make_unique<GatewayType>(to_venue_, venue_in_, clk_);

        oms_->set_limits(permissive_limits());
    }

    static risk::Limits permissive_limits() {
        risk::Limits l{};
        l.shortable = 1;
        l.max_order_qty_raw = 1'000'000;
        l.max_order_notional_e8 = 100'000'000'00'000'000LL;
        l.max_long_position_qty = 1'000'000;
        l.max_short_position_qty = 1'000'000;
        l.fat_finger_band_bp = 1000;
        l.reference_price_e8 = 100'00'000'000;
        return l;
    }

    void strategy_submit(std::uint64_t cl_ord_id, std::int64_t qty,
                        std::int64_t price_e8, Side side = Side::Buy) {
        OrderNew o{};
        o.hdr.schema_major = proto::hot::kSchemaMajor;
        o.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderNew);
        o.ids.cl_ord_id = cl_ord_id;
        o.qty_raw = qty;
        o.price_raw_e8 = price_e8;
        o.side = side;
        auto* slot = in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &o, sizeof(o));
        in_.commit();
    }

    void strategy_cancel(std::uint64_t cl_ord_id) {
        OrderCancel c{};
        c.hdr.schema_major = proto::hot::kSchemaMajor;
        c.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancel);
        c.ids.cl_ord_id = cl_ord_id;
        auto* slot = in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &c, sizeof(c));
        in_.commit();
    }

    // Run OMS and gateway alternately until neither makes progress.
    // Bounded to a sensible number of rounds so a runaway test fails fast.
    void run_until_quiescent() {
        for (int rounds = 0; rounds < 16; ++rounds) {
            const auto a = oms_->poll();
            const auto b = gw_->poll();
            if (a == 0 && b == 0) return;
        }
        FAIL() << "did not reach quiescence within 16 rounds";
    }

    template <typename Msg>
    Msg read_event() {
        const auto* slot = events_.try_read();
        EXPECT_NE(slot, nullptr);
        Msg m{};
        std::memcpy(&m, slot, sizeof(m));
        events_.release();
        return m;
    }

    [[nodiscard]] std::uint16_t next_event_type() {
        const auto* slot = events_.try_read();
        if (slot == nullptr) return 0;
        proto::hot::Header h{};
        std::memcpy(&h, slot, sizeof(h));
        return h.msg_type;
    }

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, StrategyToOms::kStorageBytes> in_storage_{};
    alignas(64) std::array<std::byte, OmsToVenue::kStorageBytes> to_venue_storage_{};
    alignas(64) std::array<std::byte, VenueToOms::kStorageBytes> venue_in_storage_{};
    alignas(64) std::array<std::byte, OmsEvents::kStorageBytes> events_storage_{};
    StrategyToOms in_{StrategyToOms::attach(in_storage_.data())};
    OmsToVenue to_venue_{OmsToVenue::attach(to_venue_storage_.data())};
    VenueToOms venue_in_{VenueToOms::attach(venue_in_storage_.data())};
    OmsEvents events_{OmsEvents::attach(events_storage_.data())};
    std::unique_ptr<OmsType> oms_;
    std::unique_ptr<GatewayType> gw_;
};

TEST_F(EndToEndFixture, BuyOrderFlowsToFilledAndUpdatesPosition) {
    strategy_submit(/*cl_ord_id=*/100, /*qty=*/50, /*price=*/100'00'000'000);
    run_until_quiescent();

    // Strategy/post-trade should observe ack then fill on the events ring.
    EXPECT_EQ(next_event_type(), static_cast<std::uint16_t>(MsgType::OrderAck));
    auto ack = read_event<OrderAck>();
    EXPECT_EQ(ack.ids.cl_ord_id, 100U);

    auto fill = read_event<OrderFill>();
    EXPECT_EQ(fill.ids.cl_ord_id, 100U);
    EXPECT_EQ(fill.fill_qty_raw, 50);

    EXPECT_EQ(oms_->position(), 50);
    EXPECT_EQ(oms_->open_orders(), 0U);
}

TEST_F(EndToEndFixture, SellOrderDecrementsPosition) {
    oms_->set_position(200);

    strategy_submit(/*cl_ord_id=*/101, /*qty=*/75, /*price=*/100'00'000'000, Side::Sell);
    run_until_quiescent();

    EXPECT_EQ(oms_->position(), 125);
    EXPECT_EQ(oms_->open_orders(), 0U);
}

TEST_F(EndToEndFixture, MultipleOrdersAllSettleCorrectly) {
    for (std::uint64_t i = 1; i <= 5; ++i) {
        strategy_submit(i, /*qty=*/10, 100'00'000'000);
    }
    run_until_quiescent();

    EXPECT_EQ(oms_->position(), 50);
    EXPECT_EQ(oms_->open_orders(), 0U);
    EXPECT_EQ(gw_->orders_acked(), 5U);
    EXPECT_EQ(gw_->fills_emitted(), 5U);
}

TEST_F(EndToEndFixture, CancelLoopUpdatesAllPartiesCorrectly) {
    // Disable auto-fill on the gateway so the order stays Working long
    // enough for a cancel to land.
    GatewayType::Behavior b;
    b.auto_fill = false;
    gw_->set_behavior(b);

    strategy_submit(/*cl_ord_id=*/200, /*qty=*/100, /*price=*/100'00'000'000);
    run_until_quiescent();

    EXPECT_EQ(oms_->open_orders(), 1U);  // Working

    strategy_cancel(200);
    run_until_quiescent();

    EXPECT_EQ(oms_->open_orders(), 0U);
    EXPECT_EQ(oms_->position(), 0);
    EXPECT_EQ(gw_->cancels_acked(), 1U);
}

TEST_F(EndToEndFixture, RiskRejectNeverReachesGateway) {
    auto l = permissive_limits();
    l.kill_switch = 1;
    oms_->set_limits(l);

    strategy_submit(/*cl_ord_id=*/300, /*qty=*/10, /*price=*/100'00'000'000);
    run_until_quiescent();

    EXPECT_EQ(gw_->orders_acked(), 0U);
    EXPECT_EQ(gw_->fills_emitted(), 0U);

    // Strategy still sees a synthesized reject on the events ring.
    EXPECT_EQ(next_event_type(),
              static_cast<std::uint16_t>(MsgType::OrderReject));
}

}  // namespace
}  // namespace ontrade::gateways
