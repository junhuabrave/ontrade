#pragma once

// MomentumStrategy — a simple example strategy demonstrating the runner API.
//
// Logic (per a single instrument, single position at a time):
//   - Track the bid price across BookUpdates.
//   - When the bid rises by >= entry_threshold_e8 from the previous quote,
//     submit a buy at the current ask.
//   - Once filled, close the position when bookkeeping shows the bid has
//     moved take_profit_bp above entry, or stop_loss_bp below entry.
//
// This is a teaching strategy, not a recommendation. Real momentum
// strategies use multi-bar features, trend confirmation, position sizing
// rules, and capacity limits — none of which belong in a one-screen example.

#include <cstdint>

#include "core/proto/hot/messages.hpp"
#include "core/strategy/runner.hpp"

namespace ontrade::strategy {

template <typename Submitter>
class MomentumStrategy : public StrategyBase<Submitter> {
public:
    struct Config {
        std::uint64_t instrument_id;
        std::int64_t qty_raw;
        std::int64_t entry_threshold_e8;  // bid uptick magnitude to enter
        std::int64_t take_profit_bp;
        std::int64_t stop_loss_bp;
    };

    enum class State : std::uint8_t {
        Flat = 0,
        PendingEntry = 1,
        Long = 2,
        PendingExit = 3,
    };

    MomentumStrategy(Submitter& s, Config c) noexcept
        : StrategyBase<Submitter>(s), cfg_(c) {}

    [[nodiscard]] State state() const noexcept { return state_; }
    [[nodiscard]] std::int64_t entry_price_e8() const noexcept { return entry_price_e8_; }

    void on_book(const proto::hot::BookUpdate& b) noexcept {
        if (b.instrument_id != cfg_.instrument_id) return;
        if (b.bid_price_e8 == 0 || b.ask_price_e8 == 0) return;

        // Seed last_bid_ on the first quote so the first comparison is valid.
        if (last_bid_ == 0) {
            last_bid_ = b.bid_price_e8;
            return;
        }

        if (state_ == State::Flat) {
            if (b.bid_price_e8 - last_bid_ >= cfg_.entry_threshold_e8) {
                const auto cl = this->submitter().submit(
                    proto::hot::Side::Buy, cfg_.instrument_id, cfg_.qty_raw,
                    b.ask_price_e8);
                if (cl != 0) {
                    state_ = State::PendingEntry;
                    entry_cl_ = cl;
                    intended_entry_e8_ = b.ask_price_e8;
                }
            }
        } else if (state_ == State::Long) {
            // bid above/below entry by N bps?
            const auto delta = b.bid_price_e8 - entry_price_e8_;
            const auto bp = (delta * 10'000) / entry_price_e8_;
            if (bp >= cfg_.take_profit_bp || bp <= -cfg_.stop_loss_bp) {
                const auto cl = this->submitter().submit(
                    proto::hot::Side::Sell, cfg_.instrument_id, cfg_.qty_raw,
                    b.bid_price_e8);
                if (cl != 0) {
                    state_ = State::PendingExit;
                    exit_cl_ = cl;
                }
            }
        }

        last_bid_ = b.bid_price_e8;
    }

    void on_fill(const proto::hot::OrderFill& f) noexcept {
        if (f.ids.cl_ord_id == entry_cl_) {
            state_ = State::Long;
            entry_price_e8_ = f.fill_price_raw_e8;
        } else if (f.ids.cl_ord_id == exit_cl_) {
            state_ = State::Flat;
            entry_cl_ = 0;
            exit_cl_ = 0;
            entry_price_e8_ = 0;
        }
    }

    void on_reject(const proto::hot::OrderReject& r) noexcept {
        if (r.ids.cl_ord_id == entry_cl_) {
            state_ = State::Flat;
            entry_cl_ = 0;
            intended_entry_e8_ = 0;
        } else if (r.ids.cl_ord_id == exit_cl_) {
            state_ = State::Long;  // still long; will retry exit on next quote
            exit_cl_ = 0;
        }
    }

private:
    Config cfg_;
    State state_{State::Flat};
    std::int64_t last_bid_{0};
    std::int64_t intended_entry_e8_{0};
    std::int64_t entry_price_e8_{0};
    std::uint64_t entry_cl_{0};
    std::uint64_t exit_cl_{0};
};

}  // namespace ontrade::strategy
