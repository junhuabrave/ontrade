#pragma once

// Minimal OMS skeleton — the first component that integrates the hot-path
// contracts: shm rings carry hot messages, the risk gate runs inline, the
// latency histogram records the cost of the gate, and timestamps populate
// the `Timestamps` field on every emitted message.
//
// Scope (intentionally small):
//   - Accepts OrderNew on the inbound ring.
//   - Runs check_pre_trade; on accept, forwards OrderNew with submit_ns
//     stamped to the outbound ring; on reject, emits an OrderReject.
//   - No order-state tracking yet — that lands with the OrderAck path.
//   - No cancel / replace / fill handling — same.
//
// Threading: the OMS is single-threaded and pinned to one isolated core
// in production. poll() drains the inbound ring; the caller drives the
// poll loop. Limits and position are mutated from the same thread (or
// at session boundaries from the control plane via a published-snapshot
// mechanism that lands later).

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "core/messaging/shm_ring.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/risk/limits.hpp"
#include "core/runtime/clock.hpp"
#include "core/runtime/latency.hpp"

namespace ontrade::oms {

// Slot size shared by inbound and outbound rings. OrderFill (128 bytes) is
// the largest hot message we ever expect a ring to carry; sizing slots to
// 128 fits any of OrderNew / OrderReject / OrderAck / OrderFill exactly.
inline constexpr std::size_t kSlotSize = 128;

static_assert(kSlotSize >= sizeof(proto::hot::OrderNew));
static_assert(kSlotSize >= sizeof(proto::hot::OrderReject));
static_assert(kSlotSize >= sizeof(proto::hot::OrderAck));
static_assert(kSlotSize >= sizeof(proto::hot::OrderFill));

template <std::size_t InboundSlots, std::size_t OutboundSlots, runtime::ClockLike Clock = runtime::SystemClock>
class Oms {
public:
    using Inbound = messaging::SpscRing<kSlotSize, InboundSlots>;
    using Outbound = messaging::SpscRing<kSlotSize, OutboundSlots>;

    Oms(Inbound& inbound, Outbound& outbound, const Clock& clk) noexcept
        : in_(inbound), out_(outbound), clk_(clk) {}

    Oms(const Oms&) = delete;
    Oms& operator=(const Oms&) = delete;
    Oms(Oms&&) = delete;
    Oms& operator=(Oms&&) = delete;

    void set_limits(const risk::Limits& l) noexcept { limits_ = l; }
    void set_position(std::int64_t qty) noexcept { position_.current_qty = qty; }
    [[nodiscard]] std::int64_t position() const noexcept { return position_.current_qty; }

    [[nodiscard]] const runtime::LatencyHistogram& risk_latency() const noexcept {
        return risk_lat_;
    }
    [[nodiscard]] std::uint64_t outbound_drops() const noexcept { return drops_; }

    // Drain the inbound ring; for each OrderNew, run pre-trade and emit
    // accept (forwarded OrderNew) or reject. Returns the number of inbound
    // messages processed in this call.
    std::size_t poll() noexcept {
        std::size_t processed = 0;
        while (const auto* slot = in_.try_read()) {
            proto::hot::Header hdr{};
            std::memcpy(&hdr, slot, sizeof(hdr));
            if (hdr.msg_type == static_cast<std::uint16_t>(proto::hot::MsgType::OrderNew)) {
                proto::hot::OrderNew order{};
                std::memcpy(&order, slot, sizeof(order));
                handle_order(order);
            }
            // Other inbound types (cancel, replace) are silently dropped at
            // this scaffold stage; they land with the order-state machine.
            in_.release();
            ++processed;
        }
        return processed;
    }

private:
    void handle_order(proto::hot::OrderNew& order) noexcept {
        runtime::stamp_hop(order.ts, runtime::HopStamp::Decision, clk_);
        const auto risk_t0 = clk_.now_ns();
        const auto outcome = risk::check_pre_trade(order, limits_, position_);
        const auto risk_t1 = clk_.now_ns();
        risk_lat_.record(risk_t1 - risk_t0);

        if (outcome.accepted()) {
            runtime::stamp_hop(order.ts, runtime::HopStamp::Submit, clk_);
            emit(order);
        } else {
            proto::hot::OrderReject rej{};
            rej.hdr.schema_major = proto::hot::kSchemaMajor;
            rej.hdr.msg_type =
                static_cast<std::uint16_t>(proto::hot::MsgType::OrderReject);
            rej.hdr.seq = next_seq_++;
            rej.ids = order.ids;
            rej.reason = outcome.reason;
            rej.ts = order.ts;
            emit(rej);
        }
    }

    template <typename Msg>
    void emit(const Msg& m) noexcept {
        auto* slot = out_.try_claim();
        if (slot == nullptr) {
            ++drops_;
            return;
        }
        std::memcpy(slot, &m, sizeof(Msg));
        out_.commit();
    }

    Inbound& in_;
    Outbound& out_;
    const Clock& clk_;
    risk::Limits limits_{};
    risk::PositionView position_{};
    runtime::LatencyHistogram risk_lat_{};
    std::uint64_t next_seq_{1};
    std::uint64_t drops_{0};
};

}  // namespace ontrade::oms
