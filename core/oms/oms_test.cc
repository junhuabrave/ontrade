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
using proto::hot::OrderNew;
using proto::hot::OrderReject;
using proto::hot::OrderRejectReason;
using proto::hot::Side;

constexpr std::size_t kSlots = 16;

class OmsFixture : public ::testing::Test {
protected:
    using InRing = messaging::SpscRing<kSlotSize, kSlots>;
    using OutRing = messaging::SpscRing<kSlotSize, kSlots>;

    void SetUp() override {
        clk_.set_wall(1'000'000);
        clk_.set_now(0);
        in_ = InRing::create(in_storage_.data());
        out_ = OutRing::create(out_storage_.data());
        oms_ = std::make_unique<Oms<kSlots, kSlots, runtime::MockClock>>(in_, out_, clk_);
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

    [[nodiscard]] proto::hot::Header read_outbound_header() {
        const auto* slot = out_.try_read();
        if (slot == nullptr) return {};
        proto::hot::Header h{};
        std::memcpy(&h, slot, sizeof(h));
        return h;
    }

    template <typename Msg>
    [[nodiscard]] Msg read_outbound() {
        const auto* slot = out_.try_read();
        EXPECT_NE(slot, nullptr);
        Msg m{};
        std::memcpy(&m, slot, sizeof(m));
        out_.release();
        return m;
    }

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, InRing::kStorageBytes> in_storage_{};
    alignas(64) std::array<std::byte, OutRing::kStorageBytes> out_storage_{};
    InRing in_{InRing::attach(in_storage_.data())};
    OutRing out_{OutRing::attach(out_storage_.data())};
    std::unique_ptr<Oms<kSlots, kSlots, runtime::MockClock>> oms_;
    std::uint64_t next_in_seq_{1};
};

TEST_F(OmsFixture, EmptyInboundProcessesNothing) {
    EXPECT_EQ(oms_->poll(), 0U);
}

TEST_F(OmsFixture, AcceptedOrderForwardedAsOrderNew) {
    push_order_new(Side::Buy, 100, 100'00'000'000);
    EXPECT_EQ(oms_->poll(), 1U);

    const auto hdr = read_outbound_header();
    EXPECT_EQ(hdr.msg_type, static_cast<std::uint16_t>(MsgType::OrderNew));
    auto fwd = read_outbound<OrderNew>();
    EXPECT_EQ(fwd.ids.cl_ord_id, 1U);
    EXPECT_EQ(fwd.qty_raw, 100);
    EXPECT_EQ(fwd.price_raw_e8, 100'00'000'000);
}

TEST_F(OmsFixture, AcceptedOrderHasSubmitStamped) {
    clk_.set_wall(7'777'777);
    push_order_new(Side::Buy, 100, 100'00'000'000);
    clk_.set_wall(7'778'000);
    oms_->poll();

    auto fwd = read_outbound<OrderNew>();
    EXPECT_EQ(fwd.ts.submit_ns, 7'778'000);
    EXPECT_EQ(fwd.ts.decision_ns, 7'778'000);
    // Origin/ingress stamps come from the producer; ensure we did not
    // overwrite them.
    EXPECT_EQ(fwd.ts.origin_ns, 7'777'777);
    EXPECT_EQ(fwd.ts.ingress_ns, 7'777'777);
}

TEST_F(OmsFixture, KillSwitchEmitsReject) {
    auto l = permissive_limits();
    l.kill_switch = 1;
    oms_->set_limits(l);

    push_order_new(Side::Buy, 100, 100'00'000'000, /*cl_ord_id=*/42);
    oms_->poll();

    auto rej = read_outbound<OrderReject>();
    EXPECT_EQ(rej.hdr.msg_type, static_cast<std::uint16_t>(MsgType::OrderReject));
    EXPECT_EQ(rej.ids.cl_ord_id, 42U);
    EXPECT_EQ(rej.reason, OrderRejectReason::KillSwitchActive);
}

TEST_F(OmsFixture, ShortWithoutLocateRejectsWithNoLocate) {
    auto l = permissive_limits();
    l.shortable = 0;
    oms_->set_limits(l);

    push_order_new(Side::Sell, 100, 100'00'000'000);
    oms_->poll();

    auto rej = read_outbound<OrderReject>();
    EXPECT_EQ(rej.reason, OrderRejectReason::NoLocate);
}

TEST_F(OmsFixture, RiskLatencyHistogramRecordsHopCost) {
    push_order_new(Side::Buy, 100, 100'00'000'000);
    push_order_new(Side::Buy, 200, 100'00'000'000);
    oms_->poll();

    EXPECT_EQ(oms_->risk_latency().total_count(), 2U);
}

TEST_F(OmsFixture, PositionUpdateAffectsCheck) {
    auto l = permissive_limits();
    l.max_long_position_qty = 50;
    oms_->set_limits(l);
    oms_->set_position(40);  // already long 40

    push_order_new(Side::Buy, 100, 100'00'000'000);  // would push to 140
    oms_->poll();

    auto rej = read_outbound<OrderReject>();
    EXPECT_EQ(rej.reason, OrderRejectReason::RiskLimitBreached);
}

TEST_F(OmsFixture, MultipleOrdersAllProcessedInSinglePoll) {
    for (std::uint64_t i = 1; i <= 5; ++i) {
        push_order_new(Side::Buy, 10, 100'00'000'000, i);
    }
    EXPECT_EQ(oms_->poll(), 5U);

    for (std::uint64_t i = 1; i <= 5; ++i) {
        auto fwd = read_outbound<OrderNew>();
        EXPECT_EQ(fwd.ids.cl_ord_id, i);
    }
}

TEST_F(OmsFixture, OutboundFullIncrementsDrops) {
    // Fill the outbound ring by claiming-but-not-releasing slots until full.
    for (std::size_t i = 0; i < kSlots; ++i) {
        auto* slot = out_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memset(slot, 0, kSlotSize);
        out_.commit();
    }
    EXPECT_EQ(out_.try_claim(), nullptr);  // truly full

    push_order_new(Side::Buy, 100, 100'00'000'000);
    oms_->poll();

    EXPECT_EQ(oms_->outbound_drops(), 1U);
}

TEST_F(OmsFixture, NonOrderNewMessagesAreSkipped) {
    // Push a Cancel-shaped header into the inbound ring; the OMS should
    // drain it without emitting anything.
    proto::hot::Header h{};
    h.schema_major = proto::hot::kSchemaMajor;
    h.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancel);
    h.seq = 99;

    auto* slot = in_.try_claim();
    ASSERT_NE(slot, nullptr);
    std::memset(slot, 0, kSlotSize);
    std::memcpy(slot, &h, sizeof(h));
    in_.commit();

    EXPECT_EQ(oms_->poll(), 1U);
    EXPECT_EQ(out_.try_read(), nullptr);  // nothing emitted
}

}  // namespace
}  // namespace ontrade::oms
