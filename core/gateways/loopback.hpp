#pragma once

// Loopback venue gateway — the test/sim peer of the OMS on the venue rings.
//
// Implements minimal "venue" behavior: assigns a monotonic exchange order id,
// optionally acks, optionally fully-fills at the order's limit price, and
// acks cancels. Behavior knobs let tests simulate slow venues, hold-orders,
// or cancellations without scenario scripting.
//
// In production this gateway is replaced by:
//   - FixGateway      (libQuickFIX wrap, control-plane order routing)
//   - OuchGateway     (hand-rolled NASDAQ OUCH binary codec)
//   - IlinkGateway    (hand-rolled CME iLink-3 codec)
// All of them implement the same ring contract: read OrderNew/OrderCancel
// from a FromOms ring (the OMS's ToVenue), emit OrderAck/OrderFill/
// OrderCancelAck on a ToOms ring (the OMS's VenueIn). The OMS does not
// know which gateway is on the other end.
//
// Threading: single-threaded, pinned to one isolated core. The gateway runs
// in its own process in production so a venue-side bug or stall cannot take
// down the OMS.

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "core/messaging/shm_ring.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/runtime/clock.hpp"

namespace ontrade::gateways {

template <std::size_t FromOmsSlots, std::size_t ToOmsSlots, runtime::ClockLike Clock = runtime::SystemClock>
class LoopbackGateway {
public:
    static constexpr std::size_t kSlotSize = proto::hot::kHotSlotBytes;
    using FromOms = messaging::SpscRing<kSlotSize, FromOmsSlots>;
    using ToOms = messaging::SpscRing<kSlotSize, ToOmsSlots>;

    // Defaults give the simplest end-to-end happy-path simulation: every
    // order acks then fills in full at its limit price; every cancel is
    // acked. Tests override per-scenario.
    struct Behavior {
        bool auto_ack = true;
        bool auto_fill = true;
        bool auto_cancel_ack = true;
    };

    LoopbackGateway(FromOms& from_oms, ToOms& to_oms, const Clock& clk) noexcept
        : from_oms_(from_oms), to_oms_(to_oms), clk_(clk) {}

    LoopbackGateway(const LoopbackGateway&) = delete;
    LoopbackGateway& operator=(const LoopbackGateway&) = delete;
    LoopbackGateway(LoopbackGateway&&) = delete;
    LoopbackGateway& operator=(LoopbackGateway&&) = delete;

    void set_behavior(const Behavior& b) noexcept { behavior_ = b; }
    [[nodiscard]] Behavior behavior() const noexcept { return behavior_; }

    [[nodiscard]] std::uint64_t orders_acked() const noexcept { return orders_acked_; }
    [[nodiscard]] std::uint64_t fills_emitted() const noexcept { return fills_emitted_; }
    [[nodiscard]] std::uint64_t cancels_acked() const noexcept { return cancels_acked_; }
    [[nodiscard]] std::uint64_t drops() const noexcept { return drops_; }

    std::size_t poll() noexcept {
        std::size_t n = 0;
        while (const auto* slot = from_oms_.try_read()) {
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
                    // OrderReplace, etc. — ignored at the loopback stage.
                    break;
            }
            from_oms_.release();
            ++n;
        }
        return n;
    }

private:
    void handle_order_new(const proto::hot::OrderNew& order) noexcept {
        const auto exch_id = ++next_exch_ord_id_;

        if (behavior_.auto_ack) {
            proto::hot::OrderAck ack{};
            ack.hdr.schema_major = proto::hot::kSchemaMajor;
            ack.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::OrderAck);
            ack.hdr.seq = next_out_seq_++;
            ack.ids = order.ids;
            ack.ids.exch_ord_id = exch_id;
            ack.ts = order.ts;
            ack.ts.ingress_ns = clk_.wall_ns();
            if (emit(ack)) {
                ++orders_acked_;
            }
        }

        if (behavior_.auto_fill) {
            proto::hot::OrderFill fill{};
            fill.hdr.schema_major = proto::hot::kSchemaMajor;
            fill.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::OrderFill);
            fill.hdr.seq = next_out_seq_++;
            fill.ids = order.ids;
            fill.ids.exch_ord_id = exch_id;
            fill.fill_qty_raw = order.qty_raw;
            fill.fill_price_raw_e8 = order.price_raw_e8;
            fill.fee_raw_e8 = 0;
            fill.liquidity = proto::hot::LiquidityFlag::Taker;
            fill.ts = order.ts;
            fill.ts.ingress_ns = clk_.wall_ns();
            if (emit(fill)) {
                ++fills_emitted_;
            }
        }
    }

    void handle_order_cancel(const proto::hot::OrderCancel& cancel) noexcept {
        if (!behavior_.auto_cancel_ack) {
            return;
        }
        proto::hot::OrderCancelAck ca{};
        ca.hdr.schema_major = proto::hot::kSchemaMajor;
        ca.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::OrderCancelAck);
        ca.hdr.seq = next_out_seq_++;
        ca.ids = cancel.ids;
        ca.ts = cancel.ts;
        ca.ts.ingress_ns = clk_.wall_ns();
        if (emit(ca)) {
            ++cancels_acked_;
        }
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

    FromOms& from_oms_;
    ToOms& to_oms_;
    const Clock& clk_;
    Behavior behavior_{};
    std::uint64_t next_exch_ord_id_{0x10000000};
    std::uint64_t next_out_seq_{1};
    std::uint64_t orders_acked_{0};
    std::uint64_t fills_emitted_{0};
    std::uint64_t cancels_acked_{0};
    std::uint64_t drops_{0};
};

}  // namespace ontrade::gateways
