#pragma once

// StrategyRunner — the hot-path host for a strategy plugin.
//
// Three rings:
//   md_in     : BookUpdate / TradeTick from md-normalizer (or book-builder)
//   events_in : OrderAck / OrderFill / OrderReject / OrderCancelAck from
//               the OMS events ring
//   to_oms    : OrderNew / OrderCancel emitted to the OMS inbound ring
//
// The strategy is held by value, owned by the runner. It receives a
// reference to the runner in its constructor and uses the runner's
// submit / cancel API to send orders. No virtuals on the call path:
// the runner's calls into the strategy resolve at compile time through
// the concrete derived type, and the strategy's calls into the runner
// resolve through the concrete StrategyRunner type.
//
// Threading: single-threaded, pinned to one isolated core. poll() drains
// md_in then events_in. The caller drives the loop.
//
// In production a strategy is loaded as a `.so` plugin; this scaffold
// puts the strategy class in the same binary as the runner so an MVP
// can be exercised end-to-end without a plugin loader. The plugin
// pathway lands later as a thin shim around this template.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

#include "core/messaging/shm_ring.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/runtime/clock.hpp"
#include "core/runtime/latency.hpp"

namespace ontrade::strategy {

// Base class with empty default callbacks. Strategies derive and override
// only the events they care about. Non-virtual: name lookup at the
// concrete type picks the derived method.
template <typename Submitter>
class StrategyBase {
public:
    explicit StrategyBase(Submitter& s) noexcept : submitter_(s) {}

    void on_book(const proto::hot::BookUpdate&) noexcept {}
    void on_trade(const proto::hot::TradeTick&) noexcept {}
    void on_ack(const proto::hot::OrderAck&) noexcept {}
    void on_fill(const proto::hot::OrderFill&) noexcept {}
    void on_reject(const proto::hot::OrderReject&) noexcept {}
    void on_cancel_ack(const proto::hot::OrderCancelAck&) noexcept {}

protected:
    [[nodiscard]] Submitter& submitter() noexcept { return submitter_; }
    [[nodiscard]] const Submitter& submitter() const noexcept { return submitter_; }

private:
    Submitter& submitter_;
};

template <
    template <typename> class StrategyTpl,
    std::size_t MdSlots,
    std::size_t EventSlots,
    std::size_t ToOmsSlots,
    runtime::ClockLike Clock = runtime::SystemClock>
class StrategyRunner {
public:
    using StrategyT = StrategyTpl<StrategyRunner>;
    using MdIn = messaging::SpscRing<proto::hot::kHotSlotBytes, MdSlots>;
    using EventsIn = messaging::SpscRing<proto::hot::kHotSlotBytes, EventSlots>;
    using ToOms = messaging::SpscRing<proto::hot::kHotSlotBytes, ToOmsSlots>;

    // The strategy is constructed in-place with (*this, args...). Extra
    // ctor args are forwarded so a strategy can take config (parameters,
    // instrument id, etc.) at construction time.
    template <typename... Args>
    StrategyRunner(MdIn& md, EventsIn& events, ToOms& to_oms,
                   const Clock& clk, Args&&... strategy_args) noexcept(
        std::is_nothrow_constructible_v<StrategyT, StrategyRunner&, Args&&...>)
        : md_(md), events_(events), to_oms_(to_oms), clk_(clk),
          strategy_(*this, std::forward<Args>(strategy_args)...) {}

    StrategyRunner(const StrategyRunner&) = delete;
    StrategyRunner& operator=(const StrategyRunner&) = delete;
    StrategyRunner(StrategyRunner&&) = delete;
    StrategyRunner& operator=(StrategyRunner&&) = delete;

    [[nodiscard]] StrategyT& strategy() noexcept { return strategy_; }
    [[nodiscard]] const StrategyT& strategy() const noexcept { return strategy_; }

    [[nodiscard]] std::uint64_t orders_submitted() const noexcept { return orders_submitted_; }
    [[nodiscard]] std::uint64_t cancels_submitted() const noexcept { return cancels_submitted_; }
    [[nodiscard]] std::uint64_t to_oms_drops() const noexcept { return drops_; }
    [[nodiscard]] std::uint64_t md_processed() const noexcept { return md_processed_; }
    [[nodiscard]] std::uint64_t events_processed() const noexcept { return events_processed_; }

    std::size_t poll() noexcept {
        return poll_md() + poll_events();
    }

    // Submitter API used by the strategy. Returns the cl_ord_id assigned
    // by the runner; the strategy stores it to correlate fills/cancels.
    // Returns 0 on emit failure (ring full); the strategy can retry.
    [[nodiscard]] std::uint64_t submit(
        proto::hot::Side side,
        std::uint64_t instrument_id,
        std::int64_t qty_raw,
        std::int64_t price_raw_e8,
        proto::hot::OrdType ord_type = proto::hot::OrdType::Limit,
        proto::hot::TimeInForce tif = proto::hot::TimeInForce::Day) noexcept {
        proto::hot::OrderNew o{};
        o.hdr.schema_major = proto::hot::kSchemaMajor;
        o.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::OrderNew);
        o.hdr.seq = next_seq_++;

        const auto cl_ord_id = ++next_cl_ord_id_;
        o.ids.cl_ord_id = cl_ord_id;
        o.ids.instrument_id = instrument_id;
        o.qty_raw = qty_raw;
        o.price_raw_e8 = price_raw_e8;
        o.side = side;
        o.ord_type = ord_type;
        o.tif = tif;

        const auto t = clk_.wall_ns();
        o.ts.origin_ns = t;
        runtime::stamp_hop(o.ts, runtime::HopStamp::Ingress, clk_);

        if (!emit(o)) {
            // Roll back the cl_ord_id so the next submit picks the same id —
            // we never sent this one. Safe because nothing else has seen it.
            --next_cl_ord_id_;
            return 0;
        }
        ++orders_submitted_;
        return cl_ord_id;
    }

    [[nodiscard]] bool cancel(std::uint64_t cl_ord_id) noexcept {
        proto::hot::OrderCancel c{};
        c.hdr.schema_major = proto::hot::kSchemaMajor;
        c.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::OrderCancel);
        c.hdr.seq = next_seq_++;
        c.ids.cl_ord_id = cl_ord_id;
        const auto t = clk_.wall_ns();
        c.ts.origin_ns = t;
        runtime::stamp_hop(c.ts, runtime::HopStamp::Ingress, clk_);

        if (!emit(c)) {
            return false;
        }
        ++cancels_submitted_;
        return true;
    }

private:
    std::size_t poll_md() noexcept {
        std::size_t n = 0;
        while (const auto* slot = md_.try_read()) {
            proto::hot::Header hdr{};
            std::memcpy(&hdr, slot, sizeof(hdr));
            switch (static_cast<proto::hot::MsgType>(hdr.msg_type)) {
                case proto::hot::MsgType::BookUpdate: {
                    proto::hot::BookUpdate b{};
                    std::memcpy(&b, slot, sizeof(b));
                    strategy_.on_book(b);
                    break;
                }
                case proto::hot::MsgType::TradeTick: {
                    proto::hot::TradeTick t{};
                    std::memcpy(&t, slot, sizeof(t));
                    strategy_.on_trade(t);
                    break;
                }
                default:
                    break;
            }
            md_.release();
            ++n;
            ++md_processed_;
        }
        return n;
    }

    std::size_t poll_events() noexcept {
        std::size_t n = 0;
        while (const auto* slot = events_.try_read()) {
            proto::hot::Header hdr{};
            std::memcpy(&hdr, slot, sizeof(hdr));
            switch (static_cast<proto::hot::MsgType>(hdr.msg_type)) {
                case proto::hot::MsgType::OrderAck: {
                    proto::hot::OrderAck a{};
                    std::memcpy(&a, slot, sizeof(a));
                    strategy_.on_ack(a);
                    break;
                }
                case proto::hot::MsgType::OrderFill: {
                    proto::hot::OrderFill f{};
                    std::memcpy(&f, slot, sizeof(f));
                    strategy_.on_fill(f);
                    break;
                }
                case proto::hot::MsgType::OrderReject: {
                    proto::hot::OrderReject r{};
                    std::memcpy(&r, slot, sizeof(r));
                    strategy_.on_reject(r);
                    break;
                }
                case proto::hot::MsgType::OrderCancelAck: {
                    proto::hot::OrderCancelAck ca{};
                    std::memcpy(&ca, slot, sizeof(ca));
                    strategy_.on_cancel_ack(ca);
                    break;
                }
                default:
                    break;
            }
            events_.release();
            ++n;
            ++events_processed_;
        }
        return n;
    }

    template <typename Msg>
    [[nodiscard]] bool emit(const Msg& m) noexcept {
        auto* slot = to_oms_.try_claim();
        if (slot == nullptr) {
            ++drops_;
            return false;
        }
        std::memcpy(slot, &m, sizeof(Msg));
        to_oms_.commit();
        return true;
    }

    MdIn& md_;
    EventsIn& events_;
    ToOms& to_oms_;
    const Clock& clk_;
    StrategyT strategy_;
    std::uint64_t next_cl_ord_id_{0};
    std::uint64_t next_seq_{1};
    std::uint64_t orders_submitted_{0};
    std::uint64_t cancels_submitted_{0};
    std::uint64_t drops_{0};
    std::uint64_t md_processed_{0};
    std::uint64_t events_processed_{0};
};

}  // namespace ontrade::strategy
