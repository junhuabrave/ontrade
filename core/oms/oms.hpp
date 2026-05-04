#pragma once

// Order-state-tracking OMS — the first integration point for the hot-path
// contracts (shm rings + hot messages + clock + latency + risk + object pool).
//
// Pipeline (four rings):
//   strategy ─inbound─▶ OMS ─to_venue─▶ gateway/venue
//   venue    ─venue_in─▶ OMS ─events──▶ strategy / post-trade / archiver
//
// The split between to_venue and events matters: each is an SPSC ring with a
// single consumer, so a venue-bound stream cannot share its slots with the
// downstream event stream that strategy and post-trade tap. Splitting also
// gives separate backpressure signals — a stuck gateway (to_venue full) is a
// different incident from a stuck downstream consumer (events full).
//
// The OMS owns:
//   - inbound  : OrderNew / OrderCancel from strategy
//   - venue_in : OrderAck / OrderFill / OrderCancelAck from venue
//   - to_venue : OrderNew / OrderCancel emitted to the gateway
//   - events   : OrderReject (synthesized) + forwarded OrderAck/Fill/CancelAck
//   - tracked-order pool : per-order state for the in-flight set, in a fixed
//                          object pool (no heap on the order path)
//
// State machine (per cl_ord_id):
//   PendingNew ─OrderAck─▶ Working ─OrderFill(remaining>0)─▶ PartiallyFilled
//   Working / PartiallyFilled ─OrderFill(remaining=0)─▶ (release)
//   Working / PartiallyFilled ─OrderCancel(in)─▶ PendingCancel
//   PendingCancel ─OrderCancelAck─▶ (release)
//
// What is NOT here yet (deliberate):
//   - replace handling
//   - venue-side OrderReject ingestion (we synthesize rejects for risk
//     failures; the path for venue-originated rejects lands when we wire a
//     real gateway)
//   - cancel-on-disconnect / session lifecycle
//   - HA handoff via Aeron archive replay
//
// Threading: single-threaded, pinned to one isolated core. poll() drains the
// inbound ring then the venue_in ring; the caller drives the loop.

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "core/memory/pool.hpp"
#include "core/messaging/shm_ring.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/risk/limits.hpp"
#include "core/runtime/archive.hpp"
#include "core/runtime/clock.hpp"
#include "core/runtime/latency.hpp"

namespace ontrade::oms {

// Slot size for the OMS rings — re-exported from proto::hot so callers
// constructing rings against this OMS can use a single name.
inline constexpr std::size_t kSlotSize = proto::hot::kHotSlotBytes;

enum class OrderState : std::uint8_t {
    PendingNew = 0,       // sent to venue, awaiting ack
    Working = 1,          // venue ack'd, no fills yet
    PartiallyFilled = 2,  // at least one partial, more to come
    PendingCancel = 3,    // cancel sent, awaiting cancel-ack
};

// Per-order state stored in the OMS pool. Compact POD; no heap, no vtable.
struct TrackedOrder {
    std::uint64_t cl_ord_id;
    std::uint64_t exch_ord_id;  // 0 until OrderAck arrives
    std::int64_t qty_total;
    std::int64_t qty_filled;
    proto::hot::Side side;
    OrderState state;
    std::uint8_t _pad[6];
};

static_assert(sizeof(TrackedOrder) == 40, "TrackedOrder layout");

// Operational snapshot of OMS state. Read by the control plane (paper-
// trading dashboard, ops UI, paging). Cheap to populate — small struct,
// counters are plain integers, per-state counts walk the in-flight set
// (bounded by MaxOpenOrders, fits in cache).
//
// Gauges describe "right now"; counters are lifetime monotonic totals
// (the control plane diffs between samples to get rates).
struct Stats {
    // Gauges
    std::size_t open_orders;
    std::size_t pending_new;
    std::size_t working;
    std::size_t partially_filled;
    std::size_t pending_cancel;
    std::int64_t position;

    // Counters (lifetime)
    std::uint64_t orders_accepted;
    std::uint64_t orders_rejected_risk;
    std::uint64_t orders_rejected_capacity;
    std::uint64_t fills_received;
    std::uint64_t cancels_acked;
    std::uint64_t to_venue_drops;
    std::uint64_t event_drops;

    // Ring queue depths (diagnostic; sustained backlog is a red flag)
    std::uint64_t inbound_pending;
    std::uint64_t venue_in_pending;
    std::uint64_t to_venue_pending;
    std::uint64_t events_pending;
};

template <
    std::size_t InboundSlots,
    std::size_t VenueInSlots,
    std::size_t ToVenueSlots,
    std::size_t EventsSlots,
    std::size_t MaxOpenOrders = 1024,
    runtime::ClockLike Clock = runtime::SystemClock>
class Oms {
public:
    using Inbound = messaging::SpscRing<kSlotSize, InboundSlots>;
    using VenueIn = messaging::SpscRing<kSlotSize, VenueInSlots>;
    using ToVenue = messaging::SpscRing<kSlotSize, ToVenueSlots>;
    using Events = messaging::SpscRing<kSlotSize, EventsSlots>;

    Oms(Inbound& inbound, VenueIn& venue_in, ToVenue& to_venue, Events& events,
        const Clock& clk, runtime::Archive* archive = nullptr) noexcept
        : in_(inbound), venue_in_(venue_in), to_venue_(to_venue),
          events_(events), clk_(clk), archive_(archive) {}

    void set_archive(runtime::Archive* archive) noexcept { archive_ = archive; }
    [[nodiscard]] runtime::Archive* archive() const noexcept { return archive_; }

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
    [[nodiscard]] std::uint64_t to_venue_drops() const noexcept { return to_venue_drops_; }
    [[nodiscard]] std::uint64_t event_drops() const noexcept { return event_drops_; }
    [[nodiscard]] std::size_t open_orders() const noexcept { return active_count_; }

    // Operational snapshot. Safe to call from the OMS thread between polls;
    // for cross-thread observation, the OMS owner should publish the result
    // into a control-plane ring (single-writer, relaxed-atomic publish).
    [[nodiscard]] Stats stats() const noexcept {
        Stats s{};
        s.open_orders = active_count_;
        s.position = position_.current_qty;
        for (std::size_t i = 0; i < active_count_; ++i) {
            switch (active_[i]->state) {
                case OrderState::PendingNew:      ++s.pending_new; break;
                case OrderState::Working:         ++s.working; break;
                case OrderState::PartiallyFilled: ++s.partially_filled; break;
                case OrderState::PendingCancel:   ++s.pending_cancel; break;
            }
        }
        s.orders_accepted = orders_accepted_;
        s.orders_rejected_risk = orders_rejected_risk_;
        s.orders_rejected_capacity = orders_rejected_capacity_;
        s.fills_received = fills_received_;
        s.cancels_acked = cancels_acked_;
        s.to_venue_drops = to_venue_drops_;
        s.event_drops = event_drops_;
        s.inbound_pending = in_.pending();
        s.venue_in_pending = venue_in_.pending();
        s.to_venue_pending = to_venue_.pending();
        s.events_pending = events_.pending();
        return s;
    }

    // Drain inbound (strategy) then venue_in (venue replies). Returns the
    // total number of messages processed in this call.
    std::size_t poll() noexcept {
        std::size_t processed = 0;
        processed += poll_inbound();
        processed += poll_venue_in();
        return processed;
    }

private:
    std::size_t poll_inbound() noexcept {
        std::size_t n = 0;
        while (const auto* slot = in_.try_read()) {
            proto::hot::Header hdr{};
            std::memcpy(&hdr, slot, sizeof(hdr));
            switch (static_cast<proto::hot::MsgType>(hdr.msg_type)) {
                case proto::hot::MsgType::OrderNew: {
                    proto::hot::OrderNew o{};
                    std::memcpy(&o, slot, sizeof(o));
                    handle_order_new(o);
                    break;
                }
                case proto::hot::MsgType::OrderCancel: {
                    proto::hot::OrderCancel c{};
                    std::memcpy(&c, slot, sizeof(c));
                    handle_order_cancel(c);
                    break;
                }
                default:
                    // OrderReplace and others are scaffold-stage drops.
                    break;
            }
            in_.release();
            ++n;
        }
        return n;
    }

    std::size_t poll_venue_in() noexcept {
        std::size_t n = 0;
        while (const auto* slot = venue_in_.try_read()) {
            proto::hot::Header hdr{};
            std::memcpy(&hdr, slot, sizeof(hdr));
            switch (static_cast<proto::hot::MsgType>(hdr.msg_type)) {
                case proto::hot::MsgType::OrderAck: {
                    proto::hot::OrderAck a{};
                    std::memcpy(&a, slot, sizeof(a));
                    handle_order_ack(a);
                    break;
                }
                case proto::hot::MsgType::OrderFill: {
                    proto::hot::OrderFill f{};
                    std::memcpy(&f, slot, sizeof(f));
                    handle_order_fill(f);
                    break;
                }
                case proto::hot::MsgType::OrderCancelAck: {
                    proto::hot::OrderCancelAck ca{};
                    std::memcpy(&ca, slot, sizeof(ca));
                    handle_order_cancel_ack(ca);
                    break;
                }
                default:
                    // Venue-originated rejects land when a real gateway is
                    // wired; skipped here so the skeleton stays small.
                    break;
            }
            venue_in_.release();
            ++n;
        }
        return n;
    }

    void handle_order_new(proto::hot::OrderNew& order) noexcept {
        runtime::stamp_hop(order.ts, runtime::HopStamp::Decision, clk_);
        const auto risk_t0 = clk_.now_ns();
        const auto outcome = risk::check_pre_trade(order, limits_, position_);
        const auto risk_t1 = clk_.now_ns();
        risk_lat_.record(risk_t1 - risk_t0);

        if (!outcome.accepted()) {
            ++orders_rejected_risk_;
            emit_reject(order, outcome.reason);
            return;
        }

        // We must track every working order to keep position correct on fills.
        // If the pool is exhausted, refuse the order rather than silently
        // forwarding an order we can't account for.
        auto* tracked = orders_pool_.construct();
        if (tracked == nullptr) {
            ++orders_rejected_capacity_;
            emit_reject(order, proto::hot::OrderRejectReason::RiskLimitBreached);
            return;
        }
        ++orders_accepted_;
        tracked->cl_ord_id = order.ids.cl_ord_id;
        tracked->exch_ord_id = 0;
        tracked->qty_total = order.qty_raw;
        tracked->qty_filled = 0;
        tracked->side = order.side;
        tracked->state = OrderState::PendingNew;
        active_[active_count_++] = tracked;

        runtime::stamp_hop(order.ts, runtime::HopStamp::Submit, clk_);
        emit_to_venue(order);
    }

    void handle_order_cancel(proto::hot::OrderCancel& cancel) noexcept {
        auto* t = find_by_cl_ord_id(cancel.ids.cl_ord_id);
        if (t == nullptr) {
            // Unknown cl_ord_id — for the skeleton we drop. A real OMS would
            // emit a CancelReject; that lands when we expand the message set.
            return;
        }
        if (t->state == OrderState::PendingCancel) {
            // Already cancelling; ignore duplicate.
            return;
        }
        t->state = OrderState::PendingCancel;
        // Echo the venue's exch_ord_id forward so the gateway can address it.
        cancel.ids.exch_ord_id = t->exch_ord_id;
        runtime::stamp_hop(cancel.ts, runtime::HopStamp::Submit, clk_);
        emit_to_venue(cancel);
    }

    void handle_order_ack(proto::hot::OrderAck& ack) noexcept {
        auto* t = find_by_cl_ord_id(ack.ids.cl_ord_id);
        if (t != nullptr) {
            t->exch_ord_id = ack.ids.exch_ord_id;
            if (t->state == OrderState::PendingNew) {
                t->state = OrderState::Working;
            }
        }
        // Forward to downstream regardless — strategy / post-trade need it.
        emit_event(ack);
    }

    void handle_order_fill(proto::hot::OrderFill& fill) noexcept {
        ++fills_received_;
        auto* t = find_by_cl_ord_id(fill.ids.cl_ord_id);
        if (t != nullptr) {
            const auto fq = fill.fill_qty_raw;
            t->qty_filled += fq;
            const std::int64_t signed_fill =
                (t->side == proto::hot::Side::Sell) ? -fq : fq;
            position_.current_qty += signed_fill;

            if (t->qty_filled >= t->qty_total) {
                release(t);
            } else if (t->state != OrderState::PendingCancel) {
                t->state = OrderState::PartiallyFilled;
            }
        }
        emit_event(fill);
    }

    void handle_order_cancel_ack(proto::hot::OrderCancelAck& ca) noexcept {
        auto* t = find_by_cl_ord_id(ca.ids.cl_ord_id);
        if (t != nullptr) {
            ++cancels_acked_;
            release(t);
        }
        emit_event(ca);
    }

    void emit_reject(const proto::hot::OrderNew& order,
                     proto::hot::OrderRejectReason reason) noexcept {
        proto::hot::OrderReject rej{};
        rej.hdr.schema_major = proto::hot::kSchemaMajor;
        rej.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::OrderReject);
        rej.hdr.seq = next_seq_++;
        rej.ids = order.ids;
        rej.reason = reason;
        rej.ts = order.ts;
        emit_event(rej);
    }

    template <typename Msg>
    void emit_to_venue(const Msg& m) noexcept {
        if (emit_into(to_venue_, m, to_venue_drops_)) {
            archive_record(runtime::RingTag::ToVenue, m);
        }
    }

    template <typename Msg>
    void emit_event(const Msg& m) noexcept {
        if (emit_into(events_, m, event_drops_)) {
            archive_record(runtime::RingTag::Events, m);
        }
    }

    template <typename Ring, typename Msg>
    [[nodiscard]] bool emit_into(Ring& ring, const Msg& m,
                                 std::uint64_t& drops) noexcept {
        auto* slot = ring.try_claim();
        if (slot == nullptr) {
            ++drops;
            return false;
        }
        std::memcpy(slot, &m, sizeof(Msg));
        ring.commit();
        return true;
    }

    // Append the just-published message to the archive if one is attached.
    // Only called on successful emit, so the archive captures exactly the
    // bytes that went on the ring (replay parity invariant). Note: this
    // tactical wiring is the producer-side hook called out in issue #4 —
    // a future refactor moves this to a separate-consumer journaler so
    // the I/O does not run on the producer's thread.
    template <typename Msg>
    void archive_record(runtime::RingTag tag, const Msg& m) noexcept {
        if (archive_ == nullptr) {
            return;
        }
        const auto* bytes = reinterpret_cast<const std::byte*>(&m);
        (void)archive_->append(
            tag,
            std::span<const std::byte>(bytes, sizeof(Msg)),
            clk_.wall_ns());
    }

    [[nodiscard]] TrackedOrder* find_by_cl_ord_id(std::uint64_t id) noexcept {
        for (std::size_t i = 0; i < active_count_; ++i) {
            if (active_[i]->cl_ord_id == id) {
                return active_[i];
            }
        }
        return nullptr;
    }

    void release(TrackedOrder* t) noexcept {
        // Swap-remove from active_ then return to pool.
        for (std::size_t i = 0; i < active_count_; ++i) {
            if (active_[i] == t) {
                active_[i] = active_[--active_count_];
                break;
            }
        }
        orders_pool_.destroy(t);
    }

    Inbound& in_;
    VenueIn& venue_in_;
    ToVenue& to_venue_;
    Events& events_;
    const Clock& clk_;
    risk::Limits limits_{};
    risk::PositionView position_{};
    runtime::LatencyHistogram risk_lat_{};
    memory::ObjectPool<TrackedOrder, MaxOpenOrders> orders_pool_{};
    std::array<TrackedOrder*, MaxOpenOrders> active_{};
    std::size_t active_count_{0};
    std::uint64_t next_seq_{1};
    std::uint64_t to_venue_drops_{0};
    std::uint64_t event_drops_{0};
    std::uint64_t orders_accepted_{0};
    std::uint64_t orders_rejected_risk_{0};
    std::uint64_t orders_rejected_capacity_{0};
    std::uint64_t fills_received_{0};
    std::uint64_t cancels_acked_{0};
    runtime::Archive* archive_{nullptr};
};

}  // namespace ontrade::oms
