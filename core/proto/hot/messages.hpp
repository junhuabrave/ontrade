#pragma once

// Hot-path wire format — fixed-offset, little-endian POD structs.
//
// Every shared-memory ring on the order path carries one of the message
// structs below; producers memcpy / placement-new into the slot, consumers
// reinterpret_cast the slot bytes. There is no vtable, no length prefix, no
// dynamic offset table — the field at byte N is always the same field at
// byte N for a given (schema_major, msg_type).
//
// This is THE contract (architecture.md §14). It is also the contract a
// future FPGA ingress/egress card writes/reads (Verilog / HLS will mirror
// the same byte layout). Do not change a struct's layout without bumping
// kSchemaMajor and recording an ADR.
//
// Endianness: little-endian native. The current target (x86_64 cloud, x86_64
// or LE-Verilog FPGA) is uniformly LE; if a BE producer ever appears we add
// byte-swap shims at that boundary, not in the wire types.
//
// Naming: raw fixed-point scalars are exposed as int64 with a documented
// scale (price_raw_e8, qty_raw). Decoding to floating-point is a caller
// concern — never on the order path.

#include <cstddef>
#include <cstdint>

namespace ontrade::proto::hot {

inline constexpr std::uint16_t kSchemaMajor = 1;
inline constexpr std::uint16_t kSchemaMinor = 0;

enum class Side : std::uint8_t {
    Buy = 0,
    Sell = 1,
};

enum class OrdType : std::uint8_t {
    Market = 0,
    Limit = 1,
    Stop = 2,
    StopLimit = 3,
    PeggedMid = 4,
};

enum class TimeInForce : std::uint8_t {
    Day = 0,
    IOC = 1,
    FOK = 2,
    GTC = 3,
    OPG = 4,  // at-the-open auction
    CLO = 5,  // at-the-close auction
};

enum class LiquidityFlag : std::uint8_t {
    Unknown = 0,
    Maker = 1,
    Taker = 2,
    Auction = 3,
};

enum class OrderRejectReason : std::uint16_t {
    Unknown = 0,
    RiskLimitBreached = 1,
    InvalidInstrument = 2,
    StalePrice = 3,
    KillSwitchActive = 4,
    DuplicateClOrdId = 5,
    NoLocate = 6,
    SessionNotOpen = 7,
    BadParameter = 8,
};

enum class MsgType : std::uint16_t {
    OrderNew = 1,
    OrderCancel = 2,
    OrderReplace = 3,
    OrderAck = 4,
    OrderReject = 5,
    OrderFill = 6,
    OrderCancelAck = 7,
};

// 16-byte header, first member of every message struct.
struct Header {
    std::uint16_t schema_major;
    std::uint16_t msg_type;
    std::uint32_t reserved;  // zero on the wire; reserved for flags
    std::uint64_t seq;       // per-ring monotonic sequence
};

// 48 bytes of identifiers. cl_ord_id is laid out as
// (host_id<<48) | (session_id<<32) | monotonic_seq so it is deterministic
// across HA failover (primary and standby produce the same id for the same
// strategy intent — required for in-flight order reconciliation).
struct Ids {
    std::uint64_t cl_ord_id;
    std::uint64_t exch_ord_id;
    std::uint64_t instrument_id;
    std::uint64_t venue_id;
    std::uint64_t account_id;
    std::uint64_t strategy_id;
};

// 32 bytes of latency-tracking timestamps (ns since UNIX epoch, wall clock).
struct Timestamps {
    std::int64_t origin_ns;    // event origin (e.g., venue feed timestamp)
    std::int64_t ingress_ns;   // time we received it
    std::int64_t decision_ns;  // time strategy decided (orders only)
    std::int64_t submit_ns;    // time we sent to venue (orders only)
};

struct OrderNew {
    Header hdr;
    Ids ids;
    std::int64_t qty_raw;
    std::int64_t price_raw_e8;  // 0 for Market
    Side side;
    OrdType ord_type;
    TimeInForce tif;
    std::uint8_t _pad[5];
    Timestamps ts;
};

struct OrderCancel {
    Header hdr;
    Ids ids;
    Timestamps ts;
};

struct OrderReplace {
    Header hdr;
    Ids ids;
    std::int64_t new_qty_raw;
    std::int64_t new_price_raw_e8;
    Timestamps ts;
};

struct OrderAck {
    Header hdr;
    Ids ids;
    Timestamps ts;
};

struct OrderReject {
    Header hdr;
    Ids ids;
    OrderRejectReason reason;
    std::uint8_t _pad[6];
    Timestamps ts;
};

struct OrderFill {
    Header hdr;
    Ids ids;
    std::int64_t fill_qty_raw;
    std::int64_t fill_price_raw_e8;
    std::int64_t fee_raw_e8;  // signed; rebates are negative
    LiquidityFlag liquidity;
    std::uint8_t _pad[7];
    Timestamps ts;
};

struct OrderCancelAck {
    Header hdr;
    Ids ids;
    Timestamps ts;
};

// Layout invariants. Any change here is a hard ABI break — bump
// kSchemaMajor and write an ADR.
static_assert(sizeof(Header) == 16, "Header layout");
static_assert(sizeof(Ids) == 48, "Ids layout");
static_assert(sizeof(Timestamps) == 32, "Timestamps layout");

static_assert(sizeof(OrderNew) == 120, "OrderNew layout");
static_assert(sizeof(OrderCancel) == 96, "OrderCancel layout");
static_assert(sizeof(OrderReplace) == 112, "OrderReplace layout");
static_assert(sizeof(OrderAck) == 96, "OrderAck layout");
static_assert(sizeof(OrderReject) == 104, "OrderReject layout");
static_assert(sizeof(OrderFill) == 128, "OrderFill layout");
static_assert(sizeof(OrderCancelAck) == 96, "OrderCancelAck layout");

// Shared slot size for any shm ring carrying these hot messages. Sized to
// fit the largest (OrderFill, 128 bytes) so a single ring can multiplex
// the full message set without per-type rings. The OMS, gateway, and any
// future strategy-runner peer all use this constant; do not bump without
// considering padding cost on smaller messages.
inline constexpr std::size_t kHotSlotBytes = 128;
static_assert(kHotSlotBytes >= sizeof(OrderNew));
static_assert(kHotSlotBytes >= sizeof(OrderCancel));
static_assert(kHotSlotBytes >= sizeof(OrderReplace));
static_assert(kHotSlotBytes >= sizeof(OrderAck));
static_assert(kHotSlotBytes >= sizeof(OrderReject));
static_assert(kHotSlotBytes >= sizeof(OrderFill));
static_assert(kHotSlotBytes >= sizeof(OrderCancelAck));

static_assert(alignof(OrderNew) == 8, "OrderNew alignment");
static_assert(alignof(OrderFill) == 8, "OrderFill alignment");

// Header offsets — pinned because FPGA Verilog will index by absolute byte.
static_assert(offsetof(Header, schema_major) == 0);
static_assert(offsetof(Header, msg_type) == 2);
static_assert(offsetof(Header, reserved) == 4);
static_assert(offsetof(Header, seq) == 8);

static_assert(offsetof(OrderNew, hdr) == 0);
static_assert(offsetof(OrderNew, ids) == 16);
static_assert(offsetof(OrderNew, qty_raw) == 64);
static_assert(offsetof(OrderNew, price_raw_e8) == 72);
static_assert(offsetof(OrderNew, side) == 80);
static_assert(offsetof(OrderNew, ord_type) == 81);
static_assert(offsetof(OrderNew, tif) == 82);
static_assert(offsetof(OrderNew, ts) == 88);

static_assert(offsetof(OrderFill, hdr) == 0);
static_assert(offsetof(OrderFill, ids) == 16);
static_assert(offsetof(OrderFill, fill_qty_raw) == 64);
static_assert(offsetof(OrderFill, fill_price_raw_e8) == 72);
static_assert(offsetof(OrderFill, fee_raw_e8) == 80);
static_assert(offsetof(OrderFill, liquidity) == 88);
static_assert(offsetof(OrderFill, ts) == 96);

}  // namespace ontrade::proto::hot
