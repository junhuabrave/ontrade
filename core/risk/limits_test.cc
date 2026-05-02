#include "core/risk/limits.hpp"

#include <gtest/gtest.h>

#include "core/proto/hot/messages.hpp"

namespace ontrade::risk {
namespace {

using proto::hot::OrderNew;
using proto::hot::OrderRejectReason;
using proto::hot::OrdType;
using proto::hot::Side;
using proto::hot::TimeInForce;

OrderNew make_order(Side side, std::int64_t qty, std::int64_t price_e8) {
    OrderNew o{};
    o.side = side;
    o.qty_raw = qty;
    o.price_raw_e8 = price_e8;
    o.ord_type = price_e8 == 0 ? OrdType::Market : OrdType::Limit;
    o.tif = TimeInForce::Day;
    return o;
}

Limits permissive_limits() {
    Limits l{};
    l.kill_switch = 0;
    l.shortable = 1;
    l.max_order_qty_raw = 1'000'000;
    l.max_order_notional_e8 = 100'000'000'00'000'000LL;  // $100M
    l.max_long_position_qty = 10'000'000;
    l.max_short_position_qty = 10'000'000;
    l.fat_finger_band_bp = 1000;  // 10%
    l.reference_price_e8 = 100'00'000'000;  // $100
    return l;
}

TEST(PreTrade, AcceptsValidLimitOrder) {
    auto o = make_order(Side::Buy, 100, 100'00'000'000);
    auto l = permissive_limits();
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, KillSwitchOverridesEverything) {
    auto o = make_order(Side::Buy, 100, 100'00'000'000);
    auto l = permissive_limits();
    l.kill_switch = 1;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::KillSwitchActive);
}

TEST(PreTrade, RejectsZeroQty) {
    auto o = make_order(Side::Buy, 0, 100'00'000'000);
    auto l = permissive_limits();
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::BadParameter);
}

TEST(PreTrade, RejectsNegativeQty) {
    auto o = make_order(Side::Buy, -50, 100'00'000'000);
    auto l = permissive_limits();
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::BadParameter);
}

TEST(PreTrade, RejectsShortWithoutLocate) {
    auto o = make_order(Side::Sell, 100, 100'00'000'000);
    auto l = permissive_limits();
    l.shortable = 0;
    PositionView p{0};  // flat -> sell goes short
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::NoLocate);
}

TEST(PreTrade, AcceptsShortWithLocate) {
    auto o = make_order(Side::Sell, 100, 100'00'000'000);
    auto l = permissive_limits();
    l.shortable = 1;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, AcceptsSellThatReducesLongRegardlessOfLocate) {
    // Selling out of a long position is not a short — locate not required.
    auto o = make_order(Side::Sell, 100, 100'00'000'000);
    auto l = permissive_limits();
    l.shortable = 0;
    PositionView p{500};  // long 500, selling 100 leaves us long 400
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, RejectsOrderQtyOverMax) {
    auto o = make_order(Side::Buy, 2'000, 100'00'000'000);
    auto l = permissive_limits();
    l.max_order_qty_raw = 1'000;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::RiskLimitBreached);
}

TEST(PreTrade, ZeroMaxOrderQtyDisablesCheck) {
    auto o = make_order(Side::Buy, 1'000'000'000, 100'00'000'000);
    auto l = permissive_limits();
    l.max_order_qty_raw = 0;
    l.max_order_notional_e8 = 0;
    l.max_long_position_qty = 0;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, RejectsNotionalOverMax) {
    auto o = make_order(Side::Buy, 100, 100'00'000'000);  // $10k notional
    auto l = permissive_limits();
    l.max_order_notional_e8 = 1'000'00'000'000;  // $1k cap
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::RiskLimitBreached);
}

TEST(PreTrade, MarketOrderSkipsNotionalCheck) {
    auto o = make_order(Side::Buy, 1'000'000, 0);  // market
    auto l = permissive_limits();
    l.max_order_notional_e8 = 100'00'000'000;  // $100 cap — irrelevant
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, NotionalOverflowGuardRejects) {
    // qty × price would overflow int64; must reject rather than UB.
    auto o = make_order(Side::Buy, 1'000'000'000'000LL, 100'000'000'000'000LL);
    auto l = permissive_limits();
    l.max_order_notional_e8 = 1'000'000'000'000'000LL;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::RiskLimitBreached);
}

TEST(PreTrade, RejectsLongPositionExceeded) {
    auto o = make_order(Side::Buy, 100, 100'00'000'000);
    auto l = permissive_limits();
    l.max_long_position_qty = 50;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::RiskLimitBreached);
}

TEST(PreTrade, AcceptsAtExactLongPositionLimit) {
    auto o = make_order(Side::Buy, 50, 100'00'000'000);
    auto l = permissive_limits();
    l.max_long_position_qty = 50;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, RejectsShortPositionExceeded) {
    auto o = make_order(Side::Sell, 100, 100'00'000'000);
    auto l = permissive_limits();
    l.shortable = 1;
    l.max_short_position_qty = 50;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::RiskLimitBreached);
}

TEST(PreTrade, RejectsFatFingerAboveBand) {
    // Reference = $100, band = 1000 bp = 10%. Order at $111 must reject.
    auto o = make_order(Side::Buy, 100, 111'00'000'000);
    auto l = permissive_limits();
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::StalePrice);
}

TEST(PreTrade, RejectsFatFingerBelowBand) {
    auto o = make_order(Side::Buy, 100, 89'00'000'000);
    auto l = permissive_limits();
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::StalePrice);
}

TEST(PreTrade, AcceptsFatFingerWithinBand) {
    auto o = make_order(Side::Buy, 100, 105'00'000'000);  // 5% above
    auto l = permissive_limits();
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, SkipsFatFingerWhenNoReference) {
    auto o = make_order(Side::Buy, 100, 1'000'00'000'000);  // wildly off
    auto l = permissive_limits();
    l.reference_price_e8 = 0;  // no reference -> skip
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, SkipsFatFingerForMarketOrders) {
    auto o = make_order(Side::Buy, 100, 0);
    auto l = permissive_limits();
    l.reference_price_e8 = 100'00'000'000;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_TRUE(out.accepted());
}

TEST(PreTrade, ChecksRunInDocumentedOrder) {
    // Kill switch must trip before any other reject.
    auto o = make_order(Side::Sell, 0, 0);  // would otherwise hit BadParameter
    auto l = permissive_limits();
    l.kill_switch = 1;
    PositionView p{0};
    auto out = check_pre_trade(o, l, p);
    EXPECT_FALSE(out.accepted());
    EXPECT_EQ(out.reason, OrderRejectReason::KillSwitchActive);
}

}  // namespace
}  // namespace ontrade::risk
