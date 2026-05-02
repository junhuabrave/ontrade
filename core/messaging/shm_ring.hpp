#pragma once

// Lock-free SPSC ring buffer with fixed-size slots over caller-supplied
// storage. The storage may live in shared memory (POSIX shm_open + mmap) so
// the same ring is visible to two processes; the ring type itself is
// storage-agnostic.
//
// Layout (single contiguous byte block):
//   [Header (cache-line aligned, padded)]
//   [slot 0][slot 1]...[slot SlotCount-1]
//
// The header keeps producer and consumer sequence counters on separate cache
// lines to avoid false sharing. Each slot is exactly SlotSize bytes of raw
// storage; the caller places a FlatBuffers / POD payload in the slot.
//
// Memory ordering:
//   - Producer: writes slot, then store-releases producer_seq.
//   - Consumer: load-acquires producer_seq, reads slot, then store-releases
//     consumer_seq.
//
// This is the contract a future FPGA card writes/reads. Do not break the
// layout without an ADR.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>

namespace ontrade::messaging {

namespace detail {
constexpr std::size_t kCacheLine = 64;
}

template <std::size_t SlotSize, std::size_t SlotCount>
class SpscRing {
    static_assert(SlotSize >= 1, "slot size must be positive");
    static_assert(SlotCount >= 2, "slot count must be at least 2");
    static_assert((SlotCount & (SlotCount - 1)) == 0,
                  "slot count must be a power of two");
    static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                  "platform must support lock-free 64-bit atomics");

public:
    static constexpr std::size_t kSlotSize = SlotSize;
    static constexpr std::size_t kSlotCount = SlotCount;

    struct alignas(detail::kCacheLine) Header {
        std::atomic<std::uint64_t> producer_seq{0};
        std::byte _pad1[detail::kCacheLine - sizeof(std::atomic<std::uint64_t>)]{};
        std::atomic<std::uint64_t> consumer_seq{0};
        std::byte _pad2[detail::kCacheLine - sizeof(std::atomic<std::uint64_t>)]{};
    };

    static constexpr std::size_t kStorageBytes =
        sizeof(Header) + SlotSize * SlotCount;

    // Creator side: zero-initializes the header in storage. The other process
    // (or thread) calls attach() afterward.
    static SpscRing create(std::byte* storage) noexcept {
        ::new (storage) Header{};
        return SpscRing(storage);
    }

    // Attacher side: assumes the header was already initialized via create().
    static SpscRing attach(std::byte* storage) noexcept {
        return SpscRing(storage);
    }

    // Producer: returns a writable slot pointer if the ring has capacity,
    // otherwise nullptr. Caller writes up to kSlotSize bytes and then calls
    // commit(). Until commit() is called the slot is reserved for this
    // producer; do not call try_claim() again before commit().
    [[nodiscard]] std::byte* try_claim() noexcept {
        const auto p = hdr_->producer_seq.load(std::memory_order_relaxed);
        const auto c = hdr_->consumer_seq.load(std::memory_order_acquire);
        if (p - c >= SlotCount) {
            return nullptr;
        }
        return slot_at(p);
    }

    void commit() noexcept {
        const auto p = hdr_->producer_seq.load(std::memory_order_relaxed);
        hdr_->producer_seq.store(p + 1, std::memory_order_release);
    }

    // Consumer: returns a readable slot pointer if a published slot is
    // available, otherwise nullptr. Caller reads up to kSlotSize bytes then
    // calls release().
    [[nodiscard]] const std::byte* try_read() const noexcept {
        const auto c = hdr_->consumer_seq.load(std::memory_order_relaxed);
        const auto p = hdr_->producer_seq.load(std::memory_order_acquire);
        if (c == p) {
            return nullptr;
        }
        return slot_at(c);
    }

    void release() noexcept {
        const auto c = hdr_->consumer_seq.load(std::memory_order_relaxed);
        hdr_->consumer_seq.store(c + 1, std::memory_order_release);
    }

    // Diagnostics — not for hot path.
    [[nodiscard]] std::uint64_t producer_seq() const noexcept {
        return hdr_->producer_seq.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t consumer_seq() const noexcept {
        return hdr_->consumer_seq.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t pending() const noexcept {
        return producer_seq() - consumer_seq();
    }

private:
    explicit SpscRing(std::byte* storage) noexcept
        : hdr_(reinterpret_cast<Header*>(storage)),
          slots_(storage + sizeof(Header)) {}

    [[nodiscard]] std::byte* slot_at(std::uint64_t seq) noexcept {
        return slots_ + (seq & (SlotCount - 1)) * SlotSize;
    }
    [[nodiscard]] const std::byte* slot_at(std::uint64_t seq) const noexcept {
        return slots_ + (seq & (SlotCount - 1)) * SlotSize;
    }

    Header* hdr_;
    std::byte* slots_;
};

}  // namespace ontrade::messaging
