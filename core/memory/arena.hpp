#pragma once

// Bump-pointer arena allocator over caller-supplied storage.
//
// Hot-path code uses arenas to satisfy "no allocations on the order path":
// the arena's storage is allocated once at session start, and each allocate()
// call is an aligned offset bump — no syscalls, no free-list walks, no locks.
// Memory is freed in bulk via reset() at a session boundary.
//
// Single-threaded by design. If a hot-path component needs cross-thread
// arena access, give each thread its own arena rather than introducing
// synchronization on the bump pointer.

#include <bit>
#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>

namespace ontrade::memory {

class Arena {
public:
    // Non-owning. `buffer` must remain valid for the arena's lifetime and be
    // suitably aligned for the largest type the caller intends to construct
    // (alignof(std::max_align_t) is sufficient for any standard type).
    Arena(std::byte* buffer, std::size_t size) noexcept
        : buffer_(buffer), size_(size), offset_(0) {}

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    Arena(Arena&&) = delete;
    Arena& operator=(Arena&&) = delete;

    [[nodiscard]] void* allocate(
        std::size_t bytes,
        std::size_t alignment = alignof(std::max_align_t)) noexcept {
        // Power-of-two alignments only — caller bug otherwise.
        if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
            return nullptr;
        }
        const std::size_t base = reinterpret_cast<std::uintptr_t>(buffer_);
        const std::size_t cur = base + offset_;
        const std::size_t aligned = (cur + alignment - 1) & ~(alignment - 1);
        const std::size_t pad = aligned - cur;
        if (pad > size_ - offset_) {
            return nullptr;
        }
        const std::size_t remaining = size_ - offset_ - pad;
        if (bytes > remaining) {
            return nullptr;
        }
        std::byte* out = buffer_ + offset_ + pad;
        offset_ += pad + bytes;
        return out;
    }

    // Allocate raw storage for one T and placement-new into it.
    // Returns nullptr if the arena is exhausted; caller must check.
    template <typename T, typename... Args>
    [[nodiscard]] T* construct(Args&&... args) noexcept(
        noexcept(T(std::forward<Args>(args)...))) {
        void* p = allocate(sizeof(T), alignof(T));
        if (p == nullptr) {
            return nullptr;
        }
        return ::new (p) T(std::forward<Args>(args)...);
    }

    // Bulk-free. Caller is responsible for ensuring no live references into
    // the arena remain. Does not invoke destructors — only use with
    // trivially-destructible types or types whose destruction is intentionally
    // skipped (POD packets, FlatBuffers buffers, etc.).
    void reset() noexcept { offset_ = 0; }

    [[nodiscard]] std::size_t used() const noexcept { return offset_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return size_; }
    [[nodiscard]] std::size_t available() const noexcept {
        return size_ - offset_;
    }

private:
    std::byte* buffer_;
    std::size_t size_;
    std::size_t offset_;
};

}  // namespace ontrade::memory
