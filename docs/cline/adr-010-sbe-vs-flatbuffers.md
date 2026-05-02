# ADR-010 — SBE vs FlatBuffers for Hot-Path Serialization

> **Created by Cline** for team review

Status: **proposed**  
Date: 2026-05-02

---

## Context

The architecture document (§5.3) specifies **zero-copy serialization** with **FlatBuffers** as the default for inter-process and on-disk messaging. However, the hot path (md-ingress → normalizer → book-builder → strategy-runner → OMS → gateway) has unique requirements:

1. **Latency budget:** < 200 µs tick-to-trade (excluding network)
2. **FPGA migration:** Future FPGA cards must read/write the same format
3. **Cache efficiency:** Fixed field positions enable better prefetching
4. **Compile-time validation:** Field offsets known at compile time

The development team has proposed **SBE (Simple Binary Encoding)** as an alternative for the hot path, keeping FlatBuffers for the control plane and archive.

---

## Decision

**Use SBE for hot-path inter-process messaging, FlatBuffers for control plane and persistence.**

### Hot Path (SBE)
- md-ingress ↔ md-normalizer
- md-normalizer ↔ book-builder
- book-builder ↔ strategy-runner
- strategy-runner ↔ OMS
- OMS ↔ venue-gateway

### Control Plane & Archive (FlatBuffers)
- Configuration messages
- Control plane RPC
- Aeron archive persistence
- Post-trade processing

---

## Consequences

### Wins

| Aspect | Benefit |
|--------|---------|
| **Cache Efficiency** | Fixed field positions = predictable cache lines, no pointer chasing |
| **FPGA Friendly** | Fixed offsets trivial to implement in Verilog; no vtable indirection |
| **Branch Prediction** | Known field positions = fewer branches, better speculation |
| **Memory Bandwidth** | No vtable overhead; smaller message sizes |
| **Zero-Copy** | SBE flyweight pattern allows direct access to shm buffer |
| **Determinism** | Message size known at compile time; no dynamic allocation |

### Costs

| Aspect | Cost |
|--------|------|
| **Schema Evolution** | Manual versioning required (SBE has no built-in forward compatibility) |
| **Tooling** | Limited reflection, JSON conversion requires custom code |
| **Complexity** | Two serialization formats in the codebase |
| **Build** | Additional code generation step in CMake |
| **Debugging** | Harder to inspect raw messages (no built-in pretty-printing) |

### Migration Path

1. **Phase 1:** Define SBE schema for core hot-path messages (MarketDataEvent, Order, Fill)
2. **Phase 2:** Implement SBE codecs alongside FlatBuffers (feature flag)
3. **Phase 3:** A/B test latency; validate FPGA compatibility
4. **Phase 4:** Switch default to SBE; keep FlatBuffers for archive

---

## Alternatives Considered

### 1. FlatBuffers Only (Status Quo)

**Pros:**
- Single format throughout
- Excellent tooling (reflection, JSON, mutation)
- Built-in schema evolution

**Cons:**
- Vtable indirection hurts cache efficiency
- Variable field positions complicate FPGA implementation
- Larger message sizes (vtable overhead)

**Verdict:** Rejected for hot path; acceptable for control plane.

### 2. Cap'n Proto

**Pros:**
- Zero-copy like FlatBuffers
- Better C++ API
- Capability-based security

**Cons:**
- Similar vtable overhead to FlatBuffers
- Less mature ecosystem
- No significant advantage over SBE for fixed-layout use case

**Verdict:** Rejected; no clear win over SBE for this use case.

### 3. Hand-Rolled Binary Protocol

**Pros:**
- Maximum control
- No external dependencies

**Cons:**
- Error-prone
- No code generation
- Maintenance burden

**Verdict:** Rejected; SBE provides code generation with minimal overhead.

### 4. Aeron's Built-in Types

**Pros:**
- Native to Aeron IPC
- No additional serialization

**Cons:**
- Limited to primitive types
- No structured message support

**Verdict:** Rejected; too limited for complex trading messages.

---

## Schema Design Guidelines

### SBE Schema Principles

1. **Fixed Block Length:** All messages have fixed size (padded if needed)
2. **No Variable-Length Fields:** Use fixed-size arrays or reference external buffers
3. **Version Field:** First field in every message for schema evolution
4. **Cache Line Alignment:** Align messages to 64-byte boundaries
5. **Group Encoding:** Use repeating groups for book levels, not nested structs

### Example Schema

**IMPORTANT:** SBE messages include an 8-byte header (block length + template ID + schema ID + version) that is NOT shown in the field layout below. Total message size = 8 (header) + sum of field sizes.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<sbe:messageSchema
    xmlns:sbe="http://fixprotocol.io/2016/sbe"
    package="ontrade.core.messages"
    id="1"
    version="1"
    semanticVersion="1.0.0"
    description="OnTrade Hot Path Messages"
    byteOrder="bigEndian">  <!-- SBE default is big-endian -->

    <types>
        <!-- Primitive types -->
        <type name="uint64" primitiveType="uint64"/>
        <type name="uint32" primitiveType="uint32"/>
        <type name="uint16" primitiveType="uint16"/>
        <type name="uint8" primitiveType="uint8"/>
        <type name="int64" primitiveType="int64"/>
        <type name="int32" primitiveType="int32"/>
        <type name="int64Price" primitiveType="int64" description="Price in 1e-9 units"/>
        <type name="int64Qty" primitiveType="int64" description="Quantity in 1e-9 units"/>
        
        <!-- Fixed-size strings -->
        <type name="SymbolId" primitiveType="uint32"/>
        <type name="VenueId" primitiveType="uint16"/>
        <type name="SessionId" primitiveType="uint32"/>
    </types>

    <!-- 
        SBE Message Layout:
        Offset  Size  Field
        0       2     Block length (size of fixed fields)
        2       2     Template ID (message type)
        4       2     Schema ID
        6       2     Schema version
        8+      N     Fixed fields (as defined below)
        
        Total MarketDataEvent size: 8 (header) + 41 (fields) + 7 (padding) = 56 bytes
        Padded to 64 bytes for cache alignment
    -->
    <sbe:message name="MarketDataEvent" id="1" description="Market data tick" blockLength="48">
        <field name="version" id="1" type="uint8" defaultValue="1" offset="0"/>
        <field name="sequence" id="2" type="uint64" offset="1"/>
        <field name="timestamp" id="3" type="uint64" description="Nanoseconds since epoch" offset="9"/>
        <field name="symbolId" id="4" type="SymbolId" offset="17"/>
        <field name="venueId" id="5" type="VenueId" offset="21"/>
        <field name="msgType" id="6" type="uint8" description="1=trade, 2=bid, 3=ask" offset="23"/>
        <field name="price" id="7" type="int64Price" offset="24"/>
        <field name="quantity" id="8" type="int64Qty" offset="32"/>
        <field name="flags" id="9" type="uint8" description="Bit flags" offset="40"/>
        <!-- Padding to 48 bytes (8-byte aligned) -->
        <field name="padding" id="10" type="uint32" offset="41"/>
    </sbe:message>

    <sbe:message name="OrderSubmit" id="2" description="Order submission" blockLength="56">
        <field name="version" id="1" type="uint8" defaultValue="1" offset="0"/>
        <field name="clOrdId" id="2" type="uint64" offset="1"/>
        <field name="timestamp" id="3" type="uint64" offset="9"/>
        <field name="symbolId" id="4" type="SymbolId" offset="17"/>
        <field name="venueId" id="5" type="VenueId" offset="21"/>
        <field name="sessionId" id="6" type="SessionId" offset="23"/>
        <field name="side" id="7" type="uint8" description="1=buy, 2=sell" offset="27"/>
        <field name="orderType" id="8" type="uint8" description="1=market, 2=limit" offset="28"/>
        <field name="price" id="9" type="int64Price" offset="29"/>
        <field name="quantity" id="10" type="int64Qty" offset="37"/>
        <field name="timeInForce" id="11" type="uint8" offset="45"/>
        <field name="flags" id="12" type="uint16" offset="46"/>
        <!-- Padding to 56 bytes -->
        <field name="padding" id="13" type="uint16" offset="48"/>
    </sbe:message>

    <sbe:message name="FillEvent" id="3" description="Order fill" blockLength="48">
        <field name="version" id="1" type="uint8" defaultValue="1" offset="0"/>
        <field name="clOrdId" id="2" type="uint64" offset="1"/>
        <field name="execId" id="3" type="uint64" offset="9"/>
        <field name="timestamp" id="4" type="uint64" offset="17"/>
        <field name="fillPrice" id="5" type="int64Price" offset="25"/>
        <field name="fillQty" id="6" type="int64Qty" offset="33"/>
        <field name="leavesQty" id="7" type="int64Qty" offset="41"/>
        <!-- Note: leavesQty at offset 41 means we need padding to 8-byte boundary -->
        <field name="flags" id="8" type="uint8" offset="49"/>
        <field name="padding" id="9" type="uint32" offset="50"/>
    </sbe:message>

</sbe:messageSchema>
```

**Endianness Note:** SBE default is big-endian. For x86_64 (little-endian), we must either:
1. Use `byteOrder="littleEndian"` in schema (non-standard extension)
2. Accept byte-swap overhead on encode/decode
3. Use FPGA that handles both endiannesses

**Recommendation:** Explicitly specify `byteOrder="littleEndian"` for OnTrade to avoid runtime byte-swap overhead.

---

## Implementation Notes

### Build Integration

```cmake
# CMakeLists.txt
find_package(sbe REQUIRED)

sbe_generate_cpp(
    SCHEMA ${CMAKE_SOURCE_DIR}/core/proto/hotpath.xml
    OUTPUT_DIR ${CMAKE_BINARY_DIR}/generated
    NAMESPACE ontrade::core::messages
)

target_include_directories(core_messages PRIVATE ${CMAKE_BINARY_DIR}/generated)
```

### Usage Example

```cpp
#include "hotpath/MarketDataEvent.h"

// Encode (producer)
void encodeEvent(char* buffer, size_t bufferLen, const Event& event) {
    ontrade::core::messages::MarketDataEvent msg;
    msg.wrapForEncode(buffer, 0, bufferLen)
       .version(1)
       .sequence(event.seq)
       .timestamp(event.ts)
       .symbolId(event.sym)
       .venueId(event.venue)
       .msgType(event.type)
       .price(event.price)
       .quantity(event.qty)
       .flags(event.flags);
    // msg is written directly to buffer - zero copy
}

// Decode (consumer)
void decodeEvent(const char* buffer, size_t bufferLen, Event& event) {
    ontrade::core::messages::MarketDataEvent msg;
    msg.wrapForDecode(buffer, 0, MarketDataEvent::sbeBlockLength(), 
                      MarketDataEvent::sbeSchemaVersion(), bufferLen);
    
    event.seq = msg.sequence();
    event.ts = msg.timestamp();
    // ... direct field access, no parsing
}
```

---

## Open Questions

1. **Schema Versioning:** How many versions to support simultaneously? (Proposed: 2)
2. **Message Size:** Fixed 64-byte messages waste space for small updates. Allow variable sizes with padding to cache line?
3. **Group Encoding:** Book updates need multiple price levels. Use SBE groups or separate messages?
4. **Archive Format:** Keep FlatBuffers for archive, or convert SBE → FlatBuffers on persistence?

---

## References

- [SBE Specification](https://github.com/FIXTradingCommunity/fix-simple-binary-encoding)
- [Real Logic SBE GitHub](https://github.com/real-logic/simple-binary-encoding)
- Architecture §5.3: Critical-path patterns
- Architecture §14: Critical contract files

---

*This ADR was created by **Cline** for team review before submission to docs/adr/*
