// ontrade_smoke — single-process MVP demonstrator.
//
// Wires the full hot-path stack (md → strategy → OMS → gateway) into one
// binary. Pushes a scripted price path through the strategy and reports
// each component's stats at the end.
//
// In production each component is its own process pinned to a dedicated
// isolated core, communicating via shm rings. Here they are objects in
// one process polled in a single thread — the ring contracts are
// identical, so the behavior under load matches what the multi-process
// build will produce.

#include <cstdint>
#include <cstring>
#include <iostream>
#include <span>
#include <string>

#include "core/gateways/loopback.hpp"
#include "core/messaging/shm_ring.hpp"
#include "core/oms/oms.hpp"
#include "core/proto/hot/messages.hpp"
#include "core/risk/limits.hpp"
#include "core/runtime/archive.hpp"
#include "core/runtime/clock.hpp"
#include "core/strategy/momentum.hpp"
#include "core/strategy/runner.hpp"

namespace {

using namespace ontrade;

constexpr std::size_t kSlotSize = proto::hot::kHotSlotBytes;
constexpr std::size_t kRingSlots = 512;
constexpr std::size_t kMaxOpenOrders = 256;

using MdRing = messaging::SpscRing<kSlotSize, kRingSlots>;
using StrategyToOms = messaging::SpscRing<kSlotSize, kRingSlots>;
using OmsToVenue = messaging::SpscRing<kSlotSize, kRingSlots>;
using VenueToOms = messaging::SpscRing<kSlotSize, kRingSlots>;
using OmsEvents = messaging::SpscRing<kSlotSize, kRingSlots>;

using OmsType = oms::Oms<kRingSlots, kRingSlots, kRingSlots, kRingSlots,
                         kMaxOpenOrders, runtime::SystemClock>;
using GatewayType = gateways::LoopbackGateway<kRingSlots, kRingSlots,
                                              runtime::SystemClock>;
using RunnerType = strategy::StrategyRunner<strategy::MomentumStrategy,
                                            kRingSlots, kRingSlots, kRingSlots,
                                            runtime::SystemClock>;

// Globals so storage is naturally 64-byte aligned and out of the stack.
alignas(64) std::byte md_storage[MdRing::kStorageBytes];
alignas(64) std::byte strat_to_oms_storage[StrategyToOms::kStorageBytes];
alignas(64) std::byte oms_to_venue_storage[OmsToVenue::kStorageBytes];
alignas(64) std::byte venue_to_oms_storage[VenueToOms::kStorageBytes];
alignas(64) std::byte oms_events_storage[OmsEvents::kStorageBytes];

risk::Limits permissive_limits() {
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

void push_book(MdRing& md, runtime::Archive* archive, runtime::SystemClock& clk,
               std::uint64_t inst, std::int64_t bid, std::int64_t ask,
               std::uint64_t seq) {
    proto::hot::BookUpdate b{};
    b.hdr.schema_major = proto::hot::kSchemaMajor;
    b.hdr.msg_type = static_cast<std::uint16_t>(proto::hot::MsgType::BookUpdate);
    b.hdr.seq = seq;
    b.instrument_id = inst;
    b.bid_price_e8 = bid;
    b.ask_price_e8 = ask;
    b.bid_qty_raw = 100;
    b.ask_qty_raw = 100;
    auto* slot = md.try_claim();
    if (slot != nullptr) {
        std::memcpy(slot, &b, sizeof(b));
        md.commit();
        if (archive != nullptr) {
            const auto* bytes = reinterpret_cast<const std::byte*>(&b);
            (void)archive->append(
                runtime::RingTag::Md,
                std::span<const std::byte>(bytes, sizeof(b)),
                clk.wall_ns());
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    runtime::SystemClock clk;

    // Optional archive path: --archive=PATH writes a journal log of every
    // ring commit. The format is documented in docs/adr/003-archive-format.md
    // and matches what app/replay (later in Stage 1.1) consumes.
    std::string archive_path;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        constexpr std::string_view kPrefix = "--archive=";
        if (arg.starts_with(kPrefix)) {
            archive_path = arg.substr(kPrefix.size());
        }
    }

    runtime::Archive archive;
    runtime::Archive* archive_ptr = nullptr;
    if (!archive_path.empty()) {
        if (!archive.open(archive_path, clk.wall_ns())) {
            std::cerr << "failed to open archive: " << archive_path << "\n";
            return 1;
        }
        archive_ptr = &archive;
        std::cout << "archive: " << archive_path << "\n";
    }

    auto md = MdRing::create(md_storage);
    auto strat_to_oms = StrategyToOms::create(strat_to_oms_storage);
    auto oms_to_venue = OmsToVenue::create(oms_to_venue_storage);
    auto venue_to_oms = VenueToOms::create(venue_to_oms_storage);
    auto oms_events = OmsEvents::create(oms_events_storage);

    OmsType oms_inst(strat_to_oms, venue_to_oms, oms_to_venue, oms_events, clk,
                     archive_ptr);
    oms_inst.set_limits(permissive_limits());

    GatewayType gw(oms_to_venue, venue_to_oms, clk, archive_ptr);

    strategy::MomentumStrategy<RunnerType>::Config cfg{};
    cfg.instrument_id = 1;
    cfg.qty_raw = 100;
    cfg.entry_threshold_e8 = 5'000'000;  // $0.05 uptick
    cfg.take_profit_bp = 50;
    cfg.stop_loss_bp = 25;

    RunnerType runner(md, oms_events, strat_to_oms, clk, cfg);
    runner.set_archive(archive_ptr);

    auto run_until_quiescent = [&] {
        for (int i = 0; i < 64; ++i) {
            const auto a = runner.poll();
            const auto b = oms_inst.poll();
            const auto c = gw.poll();
            if (a == 0 && b == 0 && c == 0) return;
        }
    };

    constexpr std::uint64_t kInst = 1;
    std::int64_t bid = 100'00'000'000;
    std::int64_t ask = 100'01'000'000;
    std::uint64_t seq = 1;

    std::cout << "ontrade_smoke — running scripted scenario\n";

    // Seed quote.
    push_book(md, archive_ptr, clk, kInst, bid, ask, seq++);
    run_until_quiescent();

    // Five small upticks (under threshold) — strategy should not trade.
    for (int i = 0; i < 5; ++i) {
        bid += 1'000'000;  // $0.01
        ask += 1'000'000;
        push_book(md, archive_ptr, clk, kInst, bid, ask, seq++);
        run_until_quiescent();
    }

    // One big uptick crossing the entry threshold ($0.05).
    bid += 10'000'000;  // $0.10
    ask += 10'000'000;
    push_book(md, archive_ptr, clk, kInst, bid, ask, seq++);
    run_until_quiescent();

    // Drift up — bid moves enough to hit the take-profit.
    for (int i = 0; i < 8; ++i) {
        bid += 10'000'000;
        ask += 10'000'000;
        push_book(md, archive_ptr, clk, kInst, bid, ask, seq++);
        run_until_quiescent();
    }

    const auto stats = oms_inst.stats();

    std::cout << "=== run summary ===\n";
    std::cout << "strategy: orders_submitted=" << runner.orders_submitted()
              << " cancels_submitted=" << runner.cancels_submitted()
              << " md_processed=" << runner.md_processed()
              << " events_processed=" << runner.events_processed()
              << " to_oms_drops=" << runner.to_oms_drops() << "\n";

    std::cout << "oms     : accepted=" << stats.orders_accepted
              << " rejected_risk=" << stats.orders_rejected_risk
              << " rejected_capacity=" << stats.orders_rejected_capacity
              << " fills_received=" << stats.fills_received
              << " open_orders=" << stats.open_orders
              << " position=" << stats.position
              << " to_venue_drops=" << stats.to_venue_drops
              << " event_drops=" << stats.event_drops << "\n";

    std::cout << "gateway : orders_acked=" << gw.orders_acked()
              << " fills_emitted=" << gw.fills_emitted()
              << " cancels_acked=" << gw.cancels_acked()
              << " drops=" << gw.drops() << "\n";

    std::cout << "strategy state=" << static_cast<int>(runner.strategy().state())
              << " entry_price_e8=" << runner.strategy().entry_price_e8() << "\n";

    if (archive_ptr != nullptr) {
        std::cout << "archive : records_written=" << archive.records_written()
                  << "\n";
        archive.close();
    }

    return 0;
}
