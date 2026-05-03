#include "core/strategy/runner.hpp"

#include <array>
#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "core/messaging/shm_ring.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/runtime/clock.hpp"
#include "core/strategy/momentum.hpp"

namespace ontrade::strategy {
namespace {

using proto::hot::BookUpdate;
using proto::hot::MsgType;
using proto::hot::OrderAck;
using proto::hot::OrderCancel;
using proto::hot::OrderCancelAck;
using proto::hot::OrderFill;
using proto::hot::OrderNew;
using proto::hot::OrderReject;
using proto::hot::OrderRejectReason;
using proto::hot::Side;
using proto::hot::TradeTick;

constexpr std::size_t kSlotSize = proto::hot::kHotSlotBytes;
constexpr std::size_t kSlots = 16;

// A test strategy that records every callback. Lets the test assert the
// runner dispatches each ring message to the correct callback.
template <typename Submitter>
class RecordingStrategy : public StrategyBase<Submitter> {
public:
    struct Counts {
        std::size_t book = 0;
        std::size_t trade = 0;
        std::size_t ack = 0;
        std::size_t fill = 0;
        std::size_t reject = 0;
        std::size_t cancel_ack = 0;
    };

    explicit RecordingStrategy(Submitter& s) noexcept : StrategyBase<Submitter>(s) {}

    void on_book(const BookUpdate& b) noexcept {
        ++counts_.book;
        last_book_ = b;
    }
    void on_trade(const TradeTick& t) noexcept {
        ++counts_.trade;
        last_trade_ = t;
    }
    void on_ack(const OrderAck& a) noexcept {
        ++counts_.ack;
        last_ack_ = a;
    }
    void on_fill(const OrderFill& f) noexcept {
        ++counts_.fill;
        last_fill_ = f;
    }
    void on_reject(const OrderReject& r) noexcept {
        ++counts_.reject;
        last_reject_ = r;
    }
    void on_cancel_ack(const OrderCancelAck& c) noexcept {
        ++counts_.cancel_ack;
        last_cancel_ack_ = c;
    }

    [[nodiscard]] const Counts& counts() const noexcept { return counts_; }
    [[nodiscard]] const BookUpdate& last_book() const noexcept { return last_book_; }
    [[nodiscard]] const TradeTick& last_trade() const noexcept { return last_trade_; }
    [[nodiscard]] const OrderAck& last_ack() const noexcept { return last_ack_; }
    [[nodiscard]] const OrderFill& last_fill() const noexcept { return last_fill_; }
    [[nodiscard]] const OrderReject& last_reject() const noexcept { return last_reject_; }

    // Convenience: forward to the runner's submit so tests can drive orders.
    [[nodiscard]] std::uint64_t submit_buy(std::uint64_t inst, std::int64_t qty,
                                           std::int64_t price_e8) noexcept {
        return this->submitter().submit(Side::Buy, inst, qty, price_e8);
    }

private:
    Counts counts_{};
    BookUpdate last_book_{};
    TradeTick last_trade_{};
    OrderAck last_ack_{};
    OrderFill last_fill_{};
    OrderReject last_reject_{};
    OrderCancelAck last_cancel_ack_{};
};

class RunnerFixture : public ::testing::Test {
protected:
    using MdRing = messaging::SpscRing<kSlotSize, kSlots>;
    using EventsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using ToOmsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using RunnerType = StrategyRunner<RecordingStrategy, kSlots, kSlots, kSlots,
                                       runtime::MockClock>;

    void SetUp() override {
        clk_.set_wall(1'000'000);
        md_ = MdRing::create(md_storage_.data());
        events_ = EventsRing::create(events_storage_.data());
        to_oms_ = ToOmsRing::create(to_oms_storage_.data());
        runner_ = std::make_unique<RunnerType>(md_, events_, to_oms_, clk_);
    }

    void push_book(std::uint64_t inst, std::int64_t bid, std::int64_t ask,
                  std::int64_t bid_qty = 100, std::int64_t ask_qty = 100) {
        BookUpdate b{};
        b.hdr.schema_major = proto::hot::kSchemaMajor;
        b.hdr.msg_type = static_cast<std::uint16_t>(MsgType::BookUpdate);
        b.instrument_id = inst;
        b.bid_price_e8 = bid;
        b.ask_price_e8 = ask;
        b.bid_qty_raw = bid_qty;
        b.ask_qty_raw = ask_qty;
        auto* slot = md_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &b, sizeof(b));
        md_.commit();
    }

    void push_trade(std::uint64_t inst, std::int64_t price, std::int64_t qty,
                   Side aggressor = Side::Buy) {
        TradeTick t{};
        t.hdr.schema_major = proto::hot::kSchemaMajor;
        t.hdr.msg_type = static_cast<std::uint16_t>(MsgType::TradeTick);
        t.instrument_id = inst;
        t.price_e8 = price;
        t.qty_raw = qty;
        t.aggressor = aggressor;
        auto* slot = md_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &t, sizeof(t));
        md_.commit();
    }

    void push_event_ack(std::uint64_t cl_ord_id, std::uint64_t exch_ord_id) {
        OrderAck a{};
        a.hdr.schema_major = proto::hot::kSchemaMajor;
        a.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderAck);
        a.ids.cl_ord_id = cl_ord_id;
        a.ids.exch_ord_id = exch_ord_id;
        auto* slot = events_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &a, sizeof(a));
        events_.commit();
    }

    void push_event_fill(std::uint64_t cl_ord_id, std::int64_t qty,
                        std::int64_t price = 100'00'000'000) {
        OrderFill f{};
        f.hdr.schema_major = proto::hot::kSchemaMajor;
        f.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderFill);
        f.ids.cl_ord_id = cl_ord_id;
        f.fill_qty_raw = qty;
        f.fill_price_raw_e8 = price;
        auto* slot = events_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &f, sizeof(f));
        events_.commit();
    }

    void push_event_reject(std::uint64_t cl_ord_id, OrderRejectReason reason) {
        OrderReject r{};
        r.hdr.schema_major = proto::hot::kSchemaMajor;
        r.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderReject);
        r.ids.cl_ord_id = cl_ord_id;
        r.reason = reason;
        auto* slot = events_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &r, sizeof(r));
        events_.commit();
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

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, MdRing::kStorageBytes> md_storage_{};
    alignas(64) std::array<std::byte, EventsRing::kStorageBytes> events_storage_{};
    alignas(64) std::array<std::byte, ToOmsRing::kStorageBytes> to_oms_storage_{};
    MdRing md_{MdRing::attach(md_storage_.data())};
    EventsRing events_{EventsRing::attach(events_storage_.data())};
    ToOmsRing to_oms_{ToOmsRing::attach(to_oms_storage_.data())};
    std::unique_ptr<RunnerType> runner_;
};

TEST_F(RunnerFixture, EmptyPollReturnsZero) {
    EXPECT_EQ(runner_->poll(), 0U);
}

TEST_F(RunnerFixture, BookUpdateDispatchedToOnBook) {
    push_book(/*inst=*/1, /*bid=*/100'00'000'000, /*ask=*/100'01'000'000);
    EXPECT_EQ(runner_->poll(), 1U);

    EXPECT_EQ(runner_->strategy().counts().book, 1U);
    EXPECT_EQ(runner_->strategy().last_book().instrument_id, 1U);
    EXPECT_EQ(runner_->strategy().last_book().bid_price_e8, 100'00'000'000);
}

TEST_F(RunnerFixture, TradeTickDispatchedToOnTrade) {
    push_trade(/*inst=*/1, 100'00'000'000, 50);
    runner_->poll();

    EXPECT_EQ(runner_->strategy().counts().trade, 1U);
    EXPECT_EQ(runner_->strategy().last_trade().qty_raw, 50);
}

TEST_F(RunnerFixture, AckFillRejectCancelAckAllDispatched) {
    push_event_ack(1, 0xABC);
    push_event_fill(1, 100);
    push_event_reject(2, OrderRejectReason::KillSwitchActive);
    OrderCancelAck ca{};
    ca.hdr.schema_major = proto::hot::kSchemaMajor;
    ca.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderCancelAck);
    ca.ids.cl_ord_id = 3;
    auto* slot = events_.try_claim();
    ASSERT_NE(slot, nullptr);
    std::memcpy(slot, &ca, sizeof(ca));
    events_.commit();

    EXPECT_EQ(runner_->poll(), 4U);
    const auto& c = runner_->strategy().counts();
    EXPECT_EQ(c.ack, 1U);
    EXPECT_EQ(c.fill, 1U);
    EXPECT_EQ(c.reject, 1U);
    EXPECT_EQ(c.cancel_ack, 1U);
}

TEST_F(RunnerFixture, SubmitProducesOrderNewOnToOms) {
    auto cl = runner_->submit(Side::Buy, 7, 100, 100'00'000'000);
    EXPECT_NE(cl, 0U);

    auto o = read_to_oms<OrderNew>();
    EXPECT_EQ(o.ids.cl_ord_id, cl);
    EXPECT_EQ(o.ids.instrument_id, 7U);
    EXPECT_EQ(o.qty_raw, 100);
    EXPECT_EQ(o.price_raw_e8, 100'00'000'000);
    EXPECT_EQ(o.side, Side::Buy);
    EXPECT_EQ(runner_->orders_submitted(), 1U);
}

TEST_F(RunnerFixture, SubmitAssignsMonotonicClOrdIds) {
    const auto a = runner_->submit(Side::Buy, 1, 10, 100'00'000'000);
    const auto b = runner_->submit(Side::Sell, 1, 10, 100'00'000'000);
    EXPECT_GT(b, a);
}

TEST_F(RunnerFixture, SubmitWithToOmsFullReturnsZeroAndCounts) {
    for (std::size_t i = 0; i < kSlots; ++i) {
        auto* slot = to_oms_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memset(slot, 0, kSlotSize);
        to_oms_.commit();
    }
    EXPECT_EQ(to_oms_.try_claim(), nullptr);

    const auto cl = runner_->submit(Side::Buy, 1, 10, 100'00'000'000);
    EXPECT_EQ(cl, 0U);
    EXPECT_EQ(runner_->orders_submitted(), 0U);
    EXPECT_EQ(runner_->to_oms_drops(), 1U);
}

TEST_F(RunnerFixture, CancelEmitsOrderCancelOnToOms) {
    EXPECT_TRUE(runner_->cancel(/*cl_ord_id=*/12345));

    auto c = read_to_oms<OrderCancel>();
    EXPECT_EQ(c.ids.cl_ord_id, 12345U);
    EXPECT_EQ(runner_->cancels_submitted(), 1U);
}

TEST_F(RunnerFixture, MdAndEventsBothDrainedInSinglePoll) {
    push_book(1, 100'00'000'000, 100'01'000'000);
    push_event_ack(1, 0xABC);
    push_trade(1, 100'00'000'000, 25);
    push_event_fill(1, 50);

    EXPECT_EQ(runner_->poll(), 4U);
    EXPECT_EQ(runner_->md_processed(), 2U);
    EXPECT_EQ(runner_->events_processed(), 2U);
}

// ---------- MomentumStrategy ----------

class MomentumFixture : public ::testing::Test {
protected:
    using MdRing = messaging::SpscRing<kSlotSize, kSlots>;
    using EventsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using ToOmsRing = messaging::SpscRing<kSlotSize, kSlots>;
    using RunnerType = StrategyRunner<MomentumStrategy, kSlots, kSlots, kSlots,
                                       runtime::MockClock>;

    void SetUp() override {
        clk_.set_wall(1'000'000);
        md_ = MdRing::create(md_storage_.data());
        events_ = EventsRing::create(events_storage_.data());
        to_oms_ = ToOmsRing::create(to_oms_storage_.data());

        MomentumStrategy<RunnerType>::Config cfg{};
        cfg.instrument_id = 1;
        cfg.qty_raw = 100;
        cfg.entry_threshold_e8 = 5'000'000;  // $0.05 uptick to enter
        cfg.take_profit_bp = 50;
        cfg.stop_loss_bp = 25;

        runner_ = std::make_unique<RunnerType>(md_, events_, to_oms_, clk_, cfg);
    }

    void push_book(std::int64_t bid, std::int64_t ask) {
        BookUpdate b{};
        b.hdr.schema_major = proto::hot::kSchemaMajor;
        b.hdr.msg_type = static_cast<std::uint16_t>(MsgType::BookUpdate);
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

    void push_event_fill(std::uint64_t cl_ord_id, std::int64_t qty,
                        std::int64_t price) {
        OrderFill f{};
        f.hdr.schema_major = proto::hot::kSchemaMajor;
        f.hdr.msg_type = static_cast<std::uint16_t>(MsgType::OrderFill);
        f.ids.cl_ord_id = cl_ord_id;
        f.fill_qty_raw = qty;
        f.fill_price_raw_e8 = price;
        auto* slot = events_.try_claim();
        ASSERT_NE(slot, nullptr);
        std::memcpy(slot, &f, sizeof(f));
        events_.commit();
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

    runtime::MockClock clk_;
    alignas(64) std::array<std::byte, MdRing::kStorageBytes> md_storage_{};
    alignas(64) std::array<std::byte, EventsRing::kStorageBytes> events_storage_{};
    alignas(64) std::array<std::byte, ToOmsRing::kStorageBytes> to_oms_storage_{};
    MdRing md_{MdRing::attach(md_storage_.data())};
    EventsRing events_{EventsRing::attach(events_storage_.data())};
    ToOmsRing to_oms_{ToOmsRing::attach(to_oms_storage_.data())};
    std::unique_ptr<RunnerType> runner_;
};

TEST_F(MomentumFixture, FirstQuoteSeedsLastBidNoTrade) {
    push_book(100'00'000'000, 100'01'000'000);
    runner_->poll();

    EXPECT_EQ(to_oms_.try_read(), nullptr);
    EXPECT_EQ(runner_->strategy().state(),
              MomentumStrategy<typename MomentumFixture::RunnerType>::State::Flat);
}

TEST_F(MomentumFixture, UptickAboveThresholdTriggersBuy) {
    using S = MomentumStrategy<typename MomentumFixture::RunnerType>;
    push_book(100'00'000'000, 100'01'000'000);  // seed
    runner_->poll();

    push_book(100'06'000'000, 100'07'000'000);  // +$0.06 uptick > $0.05 threshold
    runner_->poll();

    auto o = read_to_oms<OrderNew>();
    EXPECT_EQ(o.side, Side::Buy);
    EXPECT_EQ(o.qty_raw, 100);
    EXPECT_EQ(o.price_raw_e8, 100'07'000'000);
    EXPECT_EQ(runner_->strategy().state(), S::State::PendingEntry);
}

TEST_F(MomentumFixture, FillTransitionsPendingEntryToLong) {
    using S = MomentumStrategy<typename MomentumFixture::RunnerType>;
    push_book(100'00'000'000, 100'01'000'000);
    runner_->poll();
    push_book(100'06'000'000, 100'07'000'000);
    runner_->poll();

    auto o = read_to_oms<OrderNew>();
    push_event_fill(o.ids.cl_ord_id, 100, 100'07'000'000);
    runner_->poll();

    EXPECT_EQ(runner_->strategy().state(), S::State::Long);
    EXPECT_EQ(runner_->strategy().entry_price_e8(), 100'07'000'000);
}

TEST_F(MomentumFixture, TakeProfitTriggersExitSell) {
    using S = MomentumStrategy<typename MomentumFixture::RunnerType>;
    push_book(100'00'000'000, 100'01'000'000);
    runner_->poll();
    push_book(100'06'000'000, 100'07'000'000);
    runner_->poll();
    auto entry = read_to_oms<OrderNew>();
    push_event_fill(entry.ids.cl_ord_id, 100, 100'07'000'000);
    runner_->poll();

    // Bid moves up by 60 bps from entry — > 50 bp TP threshold. Entry was
    // 100.07; +50 bps ≈ 100.57. Push bid past that.
    push_book(100'80'000'000, 100'81'000'000);
    runner_->poll();

    auto exit_order = read_to_oms<OrderNew>();
    EXPECT_EQ(exit_order.side, Side::Sell);
    EXPECT_EQ(exit_order.qty_raw, 100);
    EXPECT_EQ(runner_->strategy().state(), S::State::PendingExit);
}

TEST_F(MomentumFixture, ExitFillReturnsToFlat) {
    using S = MomentumStrategy<typename MomentumFixture::RunnerType>;
    push_book(100'00'000'000, 100'01'000'000);
    runner_->poll();
    push_book(100'06'000'000, 100'07'000'000);
    runner_->poll();
    auto entry = read_to_oms<OrderNew>();
    push_event_fill(entry.ids.cl_ord_id, 100, 100'07'000'000);
    runner_->poll();

    push_book(100'80'000'000, 100'81'000'000);
    runner_->poll();
    auto exit_order = read_to_oms<OrderNew>();
    push_event_fill(exit_order.ids.cl_ord_id, 100, 100'80'000'000);
    runner_->poll();

    EXPECT_EQ(runner_->strategy().state(), S::State::Flat);
    EXPECT_EQ(runner_->strategy().entry_price_e8(), 0);
}

}  // namespace
}  // namespace ontrade::strategy
