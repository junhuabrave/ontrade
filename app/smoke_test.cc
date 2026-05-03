// End-to-end integration test for the full hot-path stack.
//
// Mirrors what app/smoke.cc demonstrates by hand, but asserts the
// behavior so a regression in any of strategy / OMS / gateway is caught
// in CI. This is the closest the C++ side gets to "system test" — every
// component is constructed for real and connected via real shm rings.

#include <array>
#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "core/gateways/loopback.hpp"
#include "core/messaging/shm_ring.hpp"
#include "core/oms/oms.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/risk/limits.hpp"
#include "core/runtime/clock.hpp"
#include "core/strategy/momentum.hpp"
#include "core/strategy/runner.hpp"

namespace ontrade {
namespace {

constexpr std::size_t kSlotSize = proto::hot::kHotSlotBytes;
constexpr std::size_t kSlots = 64;
constexpr std::size_t kMaxOpen = 16;

class FullStackFixture : public ::testing::Test {
protected:
    using MdRing = messaging::SpscRing<kSlotSize, kSlots>;
    using StrategyToOms = messaging::SpscRing<kSlotSize, kSlots>;
    using OmsToVenue = messaging::SpscRing<kSlotSize, kSlots>;
    using VenueToOms = messaging::SpscRing<kSlotSize, kSlots>;
    using OmsEvents = messaging::SpscRing<kSlotSize, kSlots>;

    using OmsType = oms::Oms<kSlots, kSlots, kSlots, kSlots, kMaxOpen,
                             runtime::MockClock>;
    using GatewayType = gateways::LoopbackGateway<kSlots, kSlots,
                                                  runtime::MockClock>;
    using RunnerType = strategy::StrategyRunner<strategy::MomentumStrategy,
                                                kSlots, kSlots, kSlots,
                                                runtime::MockClock>;

    void SetUp() override {
        clk_.set_wall(1'000'000);

        md_ = MdRing::create(md_storage_.data());
        s2o_ = StrategyToOms::create(s2o_storage_.data());
        o2v_ = OmsToVenue::create(o2v_storage_.data());
        v2o_ = VenueToOms::create(v2o_storage_.data());
        ev_ = OmsEvents::create(ev_storage_.data());

        oms_ = std::make_unique<OmsType>(s2o_, v2o_, o2v_, ev_, clk_);
        oms_->set_limits(permissive_limits());

        gw_ = std::make_unique<GatewayType>(o2v_, v2o_, clk_);

        strategy::MomentumStrategy<RunnerType>::Config cfg{};
        cfg.instrument_id = 1;
        cfg.qty_raw = 100;
        cfg.entry_threshold_e8 = 5'000'000;
        cfg.take_profit_bp = 50;
        cfg.stop_loss_bp = 25;
        runner_ = std::make_unique<RunnerType>(md_, ev_, s2o_, clk_, cfg);
    }

    static risk::Limits permissive_limits() {
        risk::Limits l{};
        l.shortable = 1;
        l.max_order_qty_raw = 1'000'000;
        l.max_order_notional_e8 = 1'000'000'000'000'000'000LL;
        l.max_long_position_qty = 1'000'000;
        l.max_short_position_qty = 1'000'000;
        l.fat_finger_band_bp = 1'000;
        l.reference_price_e8 = 100'00'000'000;
        return l;
    }

    void push_book(std::int64_t bid, std::int64_t ask) {
        proto::hot::BookUpdate b{};
        b.hdr.schema_major = proto::hot::kSchemaMajor;
        b.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::BookUpdate);
        b.hdr.seq = ++md_seq_;
        b.instrument_id = 1;
        b.bid_price_e8 = bid;
        b.ask_price_e8 = ask;
        b.bid_qty_raw = 100;
        b.ask_qty_raw = 100;
        auto* slot = md_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &b, sizeof(b));
        md_.commit();
    }

    void run_until_quiescent() {
        for (int i = 0; i < 64; ++i) {
            const auto a = runner_->poll();
            const auto b = oms_->poll();
            const auto c = gw_->poll();
            if (a == 0 && b == 0 && c == 0) return;
        }
        FAIL() << "did not reach quiescence within 64 rounds";
    }

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, MdRing::kStorageBytes> md_storage_{};
    alignas(64) std::array<std::byte, StrategyToOms::kStorageBytes> s2o_storage_{};
    alignas(64) std::array<std::byte, OmsToVenue::kStorageBytes> o2v_storage_{};
    alignas(64) std::array<std::byte, VenueToOms::kStorageBytes> v2o_storage_{};
    alignas(64) std::array<std::byte, OmsEvents::kStorageBytes> ev_storage_{};
    MdRing md_{MdRing::attach(md_storage_.data())};
    StrategyToOms s2o_{StrategyToOms::attach(s2o_storage_.data())};
    OmsToVenue o2v_{OmsToVenue::attach(o2v_storage_.data())};
    VenueToOms v2o_{VenueToOms::attach(v2o_storage_.data())};
    OmsEvents ev_{OmsEvents::attach(ev_storage_.data())};
    std::unique_ptr<OmsType> oms_;
    std::unique_ptr<GatewayType> gw_;
    std::unique_ptr<RunnerType> runner_;
    std::uint64_t md_seq_{0};
};

TEST_F(FullStackFixture, MomentumEntryFillsAndPositionUpdates) {
    push_book(100'00'000'000, 100'01'000'000);  // seed
    run_until_quiescent();

    push_book(100'06'000'000, 100'07'000'000);  // +$0.06 uptick triggers entry
    run_until_quiescent();

    // Entry fill should have completed, position = +qty.
    EXPECT_EQ(oms_->position(), 100);
    EXPECT_EQ(runner_->orders_submitted(), 1U);
    EXPECT_EQ(gw_->fills_emitted(), 1U);
    EXPECT_EQ(runner_->strategy().state(),
              strategy::MomentumStrategy<RunnerType>::State::Long);
    EXPECT_EQ(runner_->strategy().entry_price_e8(), 100'07'000'000);
}

TEST_F(FullStackFixture, MomentumExitOnTakeProfitReturnsFlat) {
    push_book(100'00'000'000, 100'01'000'000);
    run_until_quiescent();
    push_book(100'06'000'000, 100'07'000'000);
    run_until_quiescent();
    // Now Long at 100.07. Move bid up by ~80 bps to trigger TP (50bp).
    push_book(100'80'000'000, 100'81'000'000);
    run_until_quiescent();

    EXPECT_EQ(oms_->position(), 0);
    EXPECT_EQ(runner_->strategy().state(),
              strategy::MomentumStrategy<RunnerType>::State::Flat);
    EXPECT_EQ(runner_->orders_submitted(), 2U);  // entry + exit
    EXPECT_EQ(gw_->fills_emitted(), 2U);
}

TEST_F(FullStackFixture, RiskRejectShortCircuitsBeforeGateway) {
    auto l = permissive_limits();
    l.kill_switch = 1;
    oms_->set_limits(l);

    push_book(100'00'000'000, 100'01'000'000);
    run_until_quiescent();
    push_book(100'06'000'000, 100'07'000'000);
    run_until_quiescent();

    // Entry was attempted but rejected by risk; gateway never saw it.
    EXPECT_EQ(runner_->orders_submitted(), 1U);
    EXPECT_EQ(oms_->position(), 0);
    EXPECT_EQ(gw_->orders_acked(), 0U);
    EXPECT_EQ(gw_->fills_emitted(), 0U);
}

TEST_F(FullStackFixture, ScriptedScenarioMatchesSmokeBinary) {
    // Same scenario as app/smoke.cc — entry, exit on TP, re-entry.
    constexpr std::int64_t kStart = 100'00'000'000;
    std::int64_t bid = kStart;
    std::int64_t ask = kStart + 1'000'000;

    push_book(bid, ask);
    run_until_quiescent();

    for (int i = 0; i < 5; ++i) {
        bid += 1'000'000;
        ask += 1'000'000;
        push_book(bid, ask);
        run_until_quiescent();
    }
    // Big uptick.
    bid += 10'000'000;
    ask += 10'000'000;
    push_book(bid, ask);
    run_until_quiescent();
    // Drift up.
    for (int i = 0; i < 8; ++i) {
        bid += 10'000'000;
        ask += 10'000'000;
        push_book(bid, ask);
        run_until_quiescent();
    }

    // Final invariants — strategy entered twice and exited once → net long.
    EXPECT_EQ(runner_->orders_submitted(), 3U);
    EXPECT_EQ(gw_->fills_emitted(), 3U);
    EXPECT_EQ(oms_->position(), 100);
    EXPECT_EQ(runner_->strategy().state(),
              strategy::MomentumStrategy<RunnerType>::State::Long);
    EXPECT_EQ(oms_->open_orders(), 0U);
    EXPECT_EQ(runner_->to_oms_drops(), 0U);
}

}  // namespace
}  // namespace ontrade
