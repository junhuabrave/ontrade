#include "core/oms/oms.hpp"

#include <array>
#include <cstring>

#include <gtest/gtest.h>

#include "core/messaging/shm_ring.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/risk/limits.hpp"
#include "core/runtime/clock.hpp"

namespace ontrade::oms {
namespace {

using proto::hot::MsgType;
using proto::hot::OrderAck;
using proto::hot::OrderCancel;
using proto::hot::OrderCancelAck;
using proto::hot::OrderFill;
using proto::hot::OrderNew;
using proto::hot::OrderReject;
using proto::hot::OrderRejectReason;
using proto::hot::Side;

constexpr std::size_t kSlots = 16;
constexpr std::size_t kMaxOpen = 8;

class OmsFixture : public ::testing::Test {
protected:
    using InRing = messaging::SpscRing<kSlotSize, kSlots>;
    using VenueRing = messaging::SpscRing<kSlotSize, kSlots>;
    using ToVenueRing = messaging::SpscRing<kSlotSize, kSlots>;
    using EventsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using OmsType = Oms<kSlots, kSlots, kSlots, kSlots, kMaxOpen, runtime::MockClock>;

    void SetUp() override {
        clk_.set_wall(1'000'000);
        clk_.set_now(0);
        in_ = InRing::create(in_storage_.data());
        venue_in_ = VenueRing::create(venue_storage_.data());
        to_venue_ = ToVenueRing::create(to_venue_storage_.data());
        events_ = EventsRing::create(events_storage_.data());
        oms_ = std::make_unique<OmsType>(in_, venue_in_, to_venue_, events_, clk_);
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

    void push_order_new(Side side, std::int64_t qty, std::int64_t price_e8,
                       std::uint64_t cl_ord_id = 1) {
        OrderNew o{};
        o.hdr.schema_major = proto::hot::kSchemaMajor;
        o.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderNew);
        o.hdr.seq = next_in_seq_++;
        o.ids.cl_ord_id = cl_ord_id;
        o.side = side;
        o.qty_raw = qty;
        o.price_raw_e8 = price_e8;
        o.ord_type = price_e8 == 0 ? proto::hot::OrdType::Market
                                   : proto::hot::OrdType::Limit;
        o.tif = proto::hot::TimeInForce::Day;
        o.ts.origin_ns = clk_.wall_ns();
        runtime::stamp_hop(o.ts, runtime::HopStamp::Ingress, clk_);

        auto* slot = in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &o, sizeof(o));
        in_.commit();
    }

    void push_order_cancel(std::uint64_t cl_ord_id) {
        OrderCancel c{};
        c.hdr.schema_major = proto::hot::kSchemaMajor;
        c.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancel);
        c.hdr.seq = next_in_seq_++;
        c.ids.cl_ord_id = cl_ord_id;
        c.ts.origin_ns = clk_.wall_ns();

        auto* slot = in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &c, sizeof(c));
        in_.commit();
    }

    void push_venue_ack(std::uint64_t cl_ord_id, std::uint64_t exch_ord_id) {
        OrderAck a{};
        a.hdr.schema_major = proto::hot::kSchemaMajor;
        a.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderAck);
        a.hdr.seq = next_venue_seq_++;
        a.ids.cl_ord_id = cl_ord_id;
        a.ids.exch_ord_id = exch_ord_id;

        auto* slot = venue_in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &a, sizeof(a));
        venue_in_.commit();
    }

    void push_venue_fill(std::uint64_t cl_ord_id, std::int64_t fill_qty,
                        std::int64_t fill_price_e8 = 100'00'000'000) {
        OrderFill f{};
        f.hdr.schema_major = proto::hot::kSchemaMajor;
        f.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderFill);
        f.hdr.seq = next_venue_seq_++;
        f.ids.cl_ord_id = cl_ord_id;
        f.fill_qty_raw = fill_qty;
        f.fill_price_raw_e8 = fill_price_e8;

        auto* slot = venue_in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &f, sizeof(f));
        venue_in_.commit();
    }

    void push_venue_cancel_ack(std::uint64_t cl_ord_id) {
        OrderCancelAck ca{};
        ca.hdr.schema_major = proto::hot::kSchemaMajor;
        ca.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancelAck);
        ca.hdr.seq = next_venue_seq_++;
        ca.ids.cl_ord_id = cl_ord_id;

        auto* slot = venue_in_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &ca, sizeof(ca));
        venue_in_.commit();
    }

    [[nodiscard]] proto::hot::Header peek_to_venue_header() {
        const auto* slot = to_venue_.try_read();
        if (slot == nullptr) return {};
        proto::hot::Header h{};
        std::memcpy(&h, slot, sizeof(h));
        return h;
    }

    template <typename Msg>
    [[nodiscard]] Msg read_to_venue() {
        const auto* slot = to_venue_.try_read();
        EXPECT_NE(slot, nullptr);
        Msg m{};
        std::memcpy(&m, slot, sizeof(m));
        to_venue_.release();
        return m;
    }

    template <typename Msg>
    [[nodiscard]] Msg read_event() {
        const auto* slot = events_.try_read();
        EXPECT_NE(slot, nullptr);
        Msg m{};
        std::memcpy(&m, slot, sizeof(m));
        events_.release();
        return m;
    }

    void drain_to_venue() {
        while (to_venue_.try_read()) to_venue_.release();
    }
    void drain_events() {
        while (events_.try_read()) events_.release();
    }

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, InRing::kStorageBytes> in_storage_{};
    alignas(64) std::array<std::byte, VenueRing::kStorageBytes> venue_storage_{};
    alignas(64) std::array<std::byte, ToVenueRing::kStorageBytes> to_venue_storage_{};
    alignas(64) std::array<std::byte, EventsRing::kStorageBytes> events_storage_{};
    InRing in_{InRing::attach(in_storage_.data())};
    VenueRing venue_in_{VenueRing::attach(venue_storage_.data())};
    ToVenueRing to_venue_{ToVenueRing::attach(to_venue_storage_.data())};
    EventsRing events_{EventsRing::attach(events_storage_.data())};
    std::unique_ptr<OmsType> oms_;
    std::uint64_t next_in_seq_{1};
    std::uint64_t next_venue_seq_{1};
};

TEST_F(OmsFixture, EmptyInboundProcessesNothing) {
    EXPECT_EQ(oms_->poll(), 0U);
}

TEST_F(OmsFixture, AcceptedOrderForwardedAsOrderNew) {
    push_order_new(Side::Buy, 100, 100'00'000'000);
    EXPECT_EQ(oms_->poll(), 1U);
    EXPECT_EQ(oms_->open_orders(), 1U);

    EXPECT_EQ(peek_to_venue_header().msg_type,
              static_cast<std::uint16_t>(MsgType::OrderNew));
    auto fwd = read_to_venue<OrderNew>();
    EXPECT_EQ(fwd.ids.cl_ord_id, 1U);
    EXPECT_EQ(fwd.qty_raw, 100);
    EXPECT_EQ(fwd.price_raw_e8, 100'00'000'000);
    // The events ring should be empty for an accepted-and-forwarded order.
    EXPECT_EQ(events_.try_read(), nullptr);
}

TEST_F(OmsFixture, AcceptedOrderHasSubmitStamped) {
    clk_.set_wall(7'777'777);
    push_order_new(Side::Buy, 100, 100'00'000'000);
    clk_.set_wall(7'778'000);
    oms_->poll();

    auto fwd = read_to_venue<OrderNew>();
    EXPECT_EQ(fwd.ts.submit_ns, 7'778'000);
    EXPECT_EQ(fwd.ts.decision_ns, 7'778'000);
    EXPECT_EQ(fwd.ts.origin_ns, 7'777'777);
    EXPECT_EQ(fwd.ts.ingress_ns, 7'777'777);
}

TEST_F(OmsFixture, KillSwitchEmitsRejectOnEvents) {
    auto l = permissive_limits();
    l.kill_switch = 1;
    oms_->set_limits(l);

    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/42);
    oms_->poll();

    auto rej = read_event<OrderReject>();
    EXPECT_EQ(rej.hdr.msg_type, static_cast<std::uint16_t>(MsgType::OrderReject));
    EXPECT_EQ(rej.ids.cl_ord_id, 42U);
    EXPECT_EQ(rej.reason, OrderRejectReason::KillSwitchActive);
    EXPECT_EQ(oms_->open_orders(), 0U);
    // Rejected orders never reach the venue.
    EXPECT_EQ(to_venue_.try_read(), nullptr);
}

TEST_F(OmsFixture, ShortWithoutLocateRejectsWithNoLocate) {
    auto l = permissive_limits();
    l.shortable = 0;
    oms_->set_limits(l);

    push_order_new(Side::Sell, 100, 100'00'000'000);
    oms_->poll();

    auto rej = read_event<OrderReject>();
    EXPECT_EQ(rej.reason, OrderRejectReason::NoLocate);
}

TEST_F(OmsFixture, RiskLatencyHistogramRecordsHopCost) {
    push_order_new(Side::Buy, 100, 100'00'000'000, 1);
    push_order_new(Side::Buy, 200, 100'00'000'000, 2);
    oms_->poll();

    EXPECT_EQ(oms_->risk_latency().total_count(), 2U);
}

TEST_F(OmsFixture, PositionUpdateAffectsCheck) {
    auto l = permissive_limits();
    l.max_long_position_qty = 50;
    oms_->set_limits(l);
    oms_->set_position(40);

    push_order_new(Side::Buy, 100, 100'00'000'000);
    oms_->poll();

    auto rej = read_event<OrderReject>();
    EXPECT_EQ(rej.reason, OrderRejectReason::RiskLimitBreached);
}

TEST_F(OmsFixture, MultipleOrdersAllProcessedInSinglePoll) {
    for (std::uint64_t i = 1; i <= 5; ++i) {
        push_order_new(Side::Buy, 10, 100'00'000'000, i);
    }
    EXPECT_EQ(oms_->poll(), 5U);
    EXPECT_EQ(oms_->open_orders(), 5U);

    for (std::uint64_t i = 1; i <= 5; ++i) {
        auto fwd = read_to_venue<OrderNew>();
        EXPECT_EQ(fwd.ids.cl_ord_id, i);
    }
}

TEST_F(OmsFixture, ToVenueFullIncrementsToVenueDrops) {
    for (std::size_t i = 0; i < kSlots; ++i) {
        auto* slot = to_venue_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memset(slot, 0, kSlotSize);
        to_venue_.commit();
    }
    EXPECT_EQ(to_venue_.try_claim(), nullptr);

    push_order_new(Side::Buy, 100, 100'00'000'000);
    oms_->poll();

    EXPECT_EQ(oms_->to_venue_drops(), 1U);
    EXPECT_EQ(oms_->event_drops(), 0U);
}

TEST_F(OmsFixture, EventsFullIncrementsEventDrops) {
    auto l = permissive_limits();
    l.kill_switch = 1;
    oms_->set_limits(l);

    for (std::size_t i = 0; i < kSlots; ++i) {
        auto* slot = events_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memset(slot, 0, kSlotSize);
        events_.commit();
    }
    EXPECT_EQ(events_.try_claim(), nullptr);

    push_order_new(Side::Buy, 100, 100'00'000'000);
    oms_->poll();

    EXPECT_EQ(oms_->event_drops(), 1U);
    EXPECT_EQ(oms_->to_venue_drops(), 0U);
}

TEST_F(OmsFixture, NonOrderNewMessagesAreSkipped) {
    proto::hot::Header h{};
    h.schema_major = proto::hot::kSchemaMajor;
    h.msg_type = static_cast<std::uint16_t>(MsgType::OrderReplace);
    h.seq = 99;

    auto* slot = in_.try_claim();
    ASSERT_NE(slot, nullptr);
    std::memset(slot, 0, kSlotSize);
    std::memcpy(slot, &h, sizeof(h));
    in_.commit();

    EXPECT_EQ(oms_->poll(), 1U);
    EXPECT_EQ(to_venue_.try_read(), nullptr);
    EXPECT_EQ(events_.try_read(), nullptr);
}

// ---------- order-state machine tests ----------

TEST_F(OmsFixture, VenueAckIsForwardedToEvents) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    drain_to_venue();

    push_venue_ack(/*cl_ord_id=*/7, /*exch_ord_id=*/0xDEAD);
    EXPECT_EQ(oms_->poll(), 1U);

    auto a = read_event<OrderAck>();
    EXPECT_EQ(a.ids.cl_ord_id, 7U);
    EXPECT_EQ(a.ids.exch_ord_id, 0xDEADU);
    // Acks do not loop back to the venue.
    EXPECT_EQ(to_venue_.try_read(), nullptr);
}

TEST_F(OmsFixture, FullFillReleasesPoolSlotAndUpdatesPosition) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    drain_to_venue();

    EXPECT_EQ(oms_->open_orders(), 1U);
    EXPECT_EQ(oms_->position(), 0);

    push_venue_ack(7, 0xBEEF);
    push_venue_fill(7, 100);
    oms_->poll();

    EXPECT_EQ(oms_->open_orders(), 0U);
    EXPECT_EQ(oms_->position(), 100);
}

TEST_F(OmsFixture, PartialFillKeepsOrderOpen) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    drain_to_venue();

    push_venue_ack(7, 0xBEEF);
    push_venue_fill(7, 30);
    oms_->poll();

    EXPECT_EQ(oms_->open_orders(), 1U);
    EXPECT_EQ(oms_->position(), 30);

    push_venue_fill(7, 70);
    oms_->poll();

    EXPECT_EQ(oms_->open_orders(), 0U);
    EXPECT_EQ(oms_->position(), 100);
}

TEST_F(OmsFixture, SellFillDecrementsPosition) {
    oms_->set_position(500);

    push_order_new(Side::Sell, 200, 100'00'000'000, /*cl_ord_id=*/9);
    oms_->poll();
    drain_to_venue();

    push_venue_fill(9, 200);
    oms_->poll();

    EXPECT_EQ(oms_->position(), 300);
}

TEST_F(OmsFixture, OrderCancelForwardedWithExchOrdId) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    drain_to_venue();

    push_venue_ack(7, /*exch_ord_id=*/0xCAFE);
    oms_->poll();
    drain_events();

    push_order_cancel(7);
    EXPECT_EQ(oms_->poll(), 1U);

    auto c = read_to_venue<OrderCancel>();
    EXPECT_EQ(c.ids.cl_ord_id, 7U);
    EXPECT_EQ(c.ids.exch_ord_id, 0xCAFEU);
    EXPECT_EQ(oms_->open_orders(), 1U);
}

TEST_F(OmsFixture, CancelAckReleasesPoolSlot) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    push_order_cancel(7);
    oms_->poll();
    drain_to_venue();

    EXPECT_EQ(oms_->open_orders(), 1U);

    push_venue_cancel_ack(7);
    oms_->poll();

    EXPECT_EQ(oms_->open_orders(), 0U);
    auto ca = read_event<OrderCancelAck>();
    EXPECT_EQ(ca.ids.cl_ord_id, 7U);
}

TEST_F(OmsFixture, CancelOnUnknownClOrdIdIsSilentlyDropped) {
    push_order_cancel(/*cl_ord_id=*/9999);
    EXPECT_EQ(oms_->poll(), 1U);
    EXPECT_EQ(to_venue_.try_read(), nullptr);
    EXPECT_EQ(events_.try_read(), nullptr);
}

TEST_F(OmsFixture, DuplicateCancelIsIgnoredAfterPendingCancel) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    push_order_cancel(7);
    oms_->poll();
    drain_to_venue();

    push_order_cancel(7);
    EXPECT_EQ(oms_->poll(), 1U);
    EXPECT_EQ(to_venue_.try_read(), nullptr);
}

TEST_F(OmsFixture, PoolExhaustionEmitsRejectAndDoesNotForward) {
    for (std::uint64_t i = 1; i <= kMaxOpen; ++i) {
        push_order_new(Side::Buy, 10, 100'00'000'000, i);
    }
    oms_->poll();
    EXPECT_EQ(oms_->open_orders(), kMaxOpen);
    drain_to_venue();

    push_order_new(Side::Buy, 10, 100'00'000'000, /*cl_ord_id=*/999);
    oms_->poll();

    auto rej = read_event<OrderReject>();
    EXPECT_EQ(rej.ids.cl_ord_id, 999U);
    EXPECT_EQ(rej.reason, OrderRejectReason::RiskLimitBreached);
    EXPECT_EQ(oms_->open_orders(), kMaxOpen);
    EXPECT_EQ(to_venue_.try_read(), nullptr);
}

TEST_F(OmsFixture, FillOnUnknownClOrdIdIsForwardedWithoutCrash) {
    push_venue_fill(/*cl_ord_id=*/12345, 50);
    oms_->poll();

    auto f = read_event<OrderFill>();
    EXPECT_EQ(f.ids.cl_ord_id, 12345U);
    EXPECT_EQ(oms_->position(), 0);
    EXPECT_EQ(oms_->open_orders(), 0U);
}

TEST_F(OmsFixture, InboundAndVenueInDrainedInSinglePoll) {
    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/7);
    oms_->poll();
    drain_to_venue();

    push_order_new(Side::Buy, 50, 100'00'000'000, /*cl_ord_id=*/8);
    push_venue_ack(7, 0xAAA);
    push_venue_fill(7, 100);
    EXPECT_EQ(oms_->poll(), 3U);

    EXPECT_EQ(oms_->open_orders(), 1U);
    EXPECT_EQ(oms_->position(), 100);
}

// ---------- stats / observability ----------

TEST_F(OmsFixture, StatsStartsZeroed) {
    auto s = oms_->stats();
    EXPECT_EQ(s.open_orders, 0U);
    EXPECT_EQ(s.pending_new, 0U);
    EXPECT_EQ(s.working, 0U);
    EXPECT_EQ(s.partially_filled, 0U);
    EXPECT_EQ(s.pending_cancel, 0U);
    EXPECT_EQ(s.position, 0);
    EXPECT_EQ(s.orders_accepted, 0U);
    EXPECT_EQ(s.orders_rejected_risk, 0U);
    EXPECT_EQ(s.orders_rejected_capacity, 0U);
    EXPECT_EQ(s.fills_received, 0U);
    EXPECT_EQ(s.cancels_acked, 0U);
    EXPECT_EQ(s.to_venue_drops, 0U);
    EXPECT_EQ(s.event_drops, 0U);
}

TEST_F(OmsFixture, StatsCountsAcceptsAndRejects) {
    push_order_new(Side::Buy, 100, 100'00'000'000, 1);
    push_order_new(Side::Buy, 200, 100'00'000'000, 2);
    oms_->poll();

    auto l = permissive_limits();
    l.kill_switch = 1;
    oms_->set_limits(l);
    push_order_new(Side::Buy, 50, 100'00'000'000, 3);
    oms_->poll();

    auto s = oms_->stats();
    EXPECT_EQ(s.orders_accepted, 2U);
    EXPECT_EQ(s.orders_rejected_risk, 1U);
    EXPECT_EQ(s.orders_rejected_capacity, 0U);
}

TEST_F(OmsFixture, StatsCountsCapacityRejectsSeparately) {
    for (std::uint64_t i = 1; i <= kMaxOpen; ++i) {
        push_order_new(Side::Buy, 10, 100'00'000'000, i);
    }
    oms_->poll();
    push_order_new(Side::Buy, 10, 100'00'000'000, /*cl_ord_id=*/999);
    oms_->poll();

    auto s = oms_->stats();
    EXPECT_EQ(s.orders_accepted, kMaxOpen);
    EXPECT_EQ(s.orders_rejected_risk, 0U);
    EXPECT_EQ(s.orders_rejected_capacity, 1U);
}

TEST_F(OmsFixture, StatsBreaksDownInFlightByState) {
    for (std::uint64_t i = 1; i <= 4; ++i) {
        push_order_new(Side::Buy, 100, 100'00'000'000, i);
    }
    oms_->poll();

    push_venue_ack(1, 0xA1);
    push_venue_ack(2, 0xA2);
    push_venue_ack(3, 0xA3);
    oms_->poll();

    push_venue_fill(2, 30);
    oms_->poll();

    push_order_cancel(3);
    oms_->poll();

    auto s = oms_->stats();
    EXPECT_EQ(s.open_orders, 4U);
    EXPECT_EQ(s.working, 1U);
    EXPECT_EQ(s.partially_filled, 1U);
    EXPECT_EQ(s.pending_cancel, 1U);
    EXPECT_EQ(s.pending_new, 1U);
}

TEST_F(OmsFixture, StatsCountsFillsAndCancelAcks) {
    push_order_new(Side::Buy, 100, 100'00'000'000, 1);
    push_order_new(Side::Buy, 100, 100'00'000'000, 2);
    oms_->poll();

    push_venue_fill(1, 60);
    push_venue_fill(1, 40);
    push_order_cancel(2);
    oms_->poll();
    push_venue_cancel_ack(2);
    oms_->poll();

    auto s = oms_->stats();
    EXPECT_EQ(s.fills_received, 2U);
    EXPECT_EQ(s.cancels_acked, 1U);
    EXPECT_EQ(s.position, 100);
    EXPECT_EQ(s.open_orders, 0U);
}

TEST_F(OmsFixture, StatsReportsRingPendingDepths) {
    push_order_new(Side::Buy, 10, 100'00'000'000, 1);
    push_order_new(Side::Buy, 10, 100'00'000'000, 2);
    push_venue_fill(1, 5);

    auto s = oms_->stats();
    EXPECT_EQ(s.inbound_pending, 2U);
    EXPECT_EQ(s.venue_in_pending, 1U);
    EXPECT_EQ(s.to_venue_pending, 0U);
    EXPECT_EQ(s.events_pending, 0U);
}

}  // namespace
}  // namespace ontrade::oms
