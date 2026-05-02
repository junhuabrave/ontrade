#pragma once

// Inline pre-trade risk checks.
//
// ADR-001 places pre-trade risk inside the OMS process as a function call,
// not over a shared-memory hop: every order pays the cost, so even a 100ns
// hop would consume too much of the budget. This header is that function.
//
// Two design rules:
//   1. Single pass over a contiguous Limits struct. The cache-line layout
//      keeps the hot fields (kill_switch, max_order_*, position caps)
//      together so the entire check fits in one or two cache lines.
//   2. First failure short-circuits with a typed reason from
//      proto::hot::OrderRejectReason. The OMS turns that into an OrderReject.
//
// Limits are updated by the control plane; the hot-path reader sees them
// via a relaxed read of an atomic snapshot (see core/risk/limits_store —
// to land alongside the OMS). Eventual consistency is fine: a limit change
// takes effect within microseconds, well below human supervision latency.
//
// Killswitch is treated as a hard stop — when set, every order is rejected
// regardless of any other condition. Tested explicitly so a regression
// here cannot ship.

#include <cstdint>

#include "core/proto/hot/messages.hpp"

namespace ontrade::risk {

// Per-account / per-instrument limits. Layout is hot-fields-first so the
// kill switch and the max-order tests live in the leading cache line.
//
// All notionals and prices are int64 raw_e8 (price * 1e8). Quantities are
// int64 raw shares. Signed types throughout — short positions, rebates.
struct Limits {
    // Hard stop. Non-zero rejects every order with KillSwitchActive.
    std::uint8_t kill_switch;
    std::uint8_t shortable;            // non-zero if a short locate is held
    std::uint8_t _pad0[6];

    // Per-order ceilings. 0 disables the check.
    std::int64_t max_order_qty_raw;
    std::int64_t max_order_notional_e8;

    // Position ceilings. Both are non-negative magnitudes; the check on
    // the short side compares against -max_short_position_qty.
    std::int64_t max_long_position_qty;
    std::int64_t max_short_position_qty;

    // Fat-finger price band. order_price must satisfy
    //   |order_price - reference_price| * 10000 <= reference_price * fat_finger_band_bp
    // i.e. within fat_finger_band_bp basis points of the reference. If
    // reference_price_e8 == 0 the check is skipped (no reference yet —
    // pre-open, halted, illiquid name).
    std::int64_t fat_finger_band_bp;
    std::int64_t reference_price_e8;
};

// Position snapshot the OMS passes in. Signed: positive long, negative
// short. The OMS owns position state; this struct is a per-call view.
struct PositionView {
    std::int64_t current_qty;
};

// Outcome of a single check. The caller (OMS) constructs an OrderReject
// from the reason on a Reject result.
struct CheckOutcome {
    enum class Result : std::uint8_t { Accept = 0, Reject = 1 };
    Result result;
    proto::hot::OrderRejectReason reason;

    [[nodiscard]] constexpr bool accepted() const noexcept {
        return result == Result::Accept;
    }
};

[[nodiscard]] inline constexpr CheckOutcome accept() noexcept {
    return {CheckOutcome::Result::Accept, proto::hot::OrderRejectReason::Unknown};
}

[[nodiscard]] inline constexpr CheckOutcome reject(
    proto::hot::OrderRejectReason r) noexcept {
    return {CheckOutcome::Result::Reject, r};
}

// THE inline pre-trade check. Sub-µs target, no allocations, no syscalls.
//
// Order of checks is intentional: kill switch first (cheapest, shadows
// every other answer); locate-for-short next (regulatory hard stop);
// then the order-shape ceilings (qty, notional); then the position-after
// ceilings; then fat-finger last (most expensive math, catches what the
// other checks won't).
[[nodiscard]] inline CheckOutcome check_pre_trade(
    const proto::hot::OrderNew& order,
    const Limits& limits,
    const PositionView& pos) noexcept {
    using proto::hot::OrderRejectReason;
    using proto::hot::Side;

    if (limits.kill_switch != 0) {
        return reject(OrderRejectReason::KillSwitchActive);
    }

    if (order.side == Side::Sell && order.price_raw_e8 != 0) {
        // Short-sale flag: the simple model is "any sell that crosses our
        // book into short territory needs a locate". Without a true book
        // here we use the conservative position-based test below; the
        // explicit shortable flag is a hard regulatory stop honored
        // whenever the post-fill position would be short.
    }

    // Short-locate enforcement: if the order would push us net short and we
    // do not hold a locate, refuse.
    const auto qty = order.qty_raw;
    if (qty <= 0) {
        return reject(OrderRejectReason::BadParameter);
    }

    const std::int64_t signed_qty =
        (order.side == Side::Sell) ? -qty : qty;
    const std::int64_t post_qty = pos.current_qty + signed_qty;
    if (post_qty < 0 && limits.shortable == 0) {
        return reject(OrderRejectReason::NoLocate);
    }

    // Per-order ceilings. 0 disables.
    if (limits.max_order_qty_raw > 0 && qty > limits.max_order_qty_raw) {
        return reject(OrderRejectReason::RiskLimitBreached);
    }
    if (limits.max_order_notional_e8 > 0 && order.price_raw_e8 > 0) {
        // qty * price_e8 fits comfortably in int64 for sane equity sizes
        // (1e9 shares * 1e12 cents = 1e21, exceeds int64). Cap explicitly.
        // The realistic ceiling is qty <= 1e10 and price_e8 <= 1e12, so
        // notional <= 1e22 which DOES overflow. Detect and reject.
        constexpr std::int64_t kSafeMax = 1'000'000'000'000'000'000LL;  // 1e18
        if (qty > kSafeMax / order.price_raw_e8) {
            return reject(OrderRejectReason::RiskLimitBreached);
        }
        const std::int64_t notional_e8 = qty * order.price_raw_e8;
        if (notional_e8 > limits.max_order_notional_e8) {
            return reject(OrderRejectReason::RiskLimitBreached);
        }
    }

    // Position ceilings.
    if (limits.max_long_position_qty > 0 &&
        post_qty > limits.max_long_position_qty) {
        return reject(OrderRejectReason::RiskLimitBreached);
    }
    if (limits.max_short_position_qty > 0 &&
        post_qty < -limits.max_short_position_qty) {
        return reject(OrderRejectReason::RiskLimitBreached);
    }

    // Fat-finger band. Skip when no reference yet, or for market orders
    // (price_raw_e8 == 0).
    if (order.price_raw_e8 != 0 &&
        limits.reference_price_e8 != 0 &&
        limits.fat_finger_band_bp > 0) {
        const std::int64_t diff = order.price_raw_e8 > limits.reference_price_e8
            ? order.price_raw_e8 - limits.reference_price_e8
            : limits.reference_price_e8 - order.price_raw_e8;
        // diff * 10000 <= reference * band_bp  ⇔  within band.
        // Both sides fit in int64 for reasonable price scales.
        constexpr std::int64_t kBpScale = 10'000;
        if (diff > kBpScale) {
            // Avoid overflow on diff*kBpScale by comparing as rationals.
            // diff/reference > band_bp/10000  ⇔  diff*10000 > reference*band_bp
            const std::int64_t lhs_cap = limits.reference_price_e8 / kBpScale * limits.fat_finger_band_bp;
            if (diff > lhs_cap) {
                return reject(OrderRejectReason::StalePrice);
            }
        }
    }

    return accept();
}

}  // namespace ontrade::risk
