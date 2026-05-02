#pragma once

// Fixed-capacity object pool with a stack-based free list.
//
// Pre-allocates raw aligned storage for `Capacity` objects of type T at
// construction. Hot-path callers acquire/release via the construct/destroy
// pair; no heap traffic on the order path.
//
// Single-threaded by design. The OMS, strategy-runner, and gateway each own
// their pools and access them from a single pinned thread per process; there
// is no cross-thread contention to synchronize.

#include <array>
#include <cstddef>
#include <new>
#include <utility>

namespace ontrade::memory {

template <typename T, std::size_t Capacity>
class ObjectPool {
    static_assert(Capacity >= 1, "capacity must be positive");

public:
    ObjectPool() noexcept {
        for (std::size_t i = 0; i < Capacity; ++i) {
            free_list_[i] = slot_ptr(i);
        }
        free_count_ = Capacity;
    }

    ~ObjectPool() = default;

    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;
    ObjectPool(ObjectPool&&) = delete;
    ObjectPool& operator=(ObjectPool&&) = delete;

    // Construct an object in a free slot. Returns nullptr if the pool is
    // exhausted. Caller releases via destroy().
    template <typename... Args>
    [[nodiscard]] T* construct(Args&&... args) noexcept(
        noexcept(T(std::forward<Args>(args)...))) {
        if (free_count_ == 0) {
            return nullptr;
        }
        T* slot = free_list_[--free_count_];
        return ::new (static_cast<void*>(slot)) T(std::forward<Args>(args)...);
    }

    // Destroy and return to the pool. nullptr is a no-op. The pointer must
    // have been returned by construct() on this same pool — passing a foreign
    // pointer is undefined.
    void destroy(T* obj) noexcept {
        if (obj == nullptr) {
            return;
        }
        obj->~T();
        free_list_[free_count_++] = obj;
    }

    [[nodiscard]] std::size_t free_slots() const noexcept {
        return free_count_;
    }
    [[nodiscard]] std::size_t in_use() const noexcept {
        return Capacity - free_count_;
    }
    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }

private:
    [[nodiscard]] T* slot_ptr(std::size_t i) noexcept {
        return reinterpret_cast<T*>(&storage_[i]);
    }

    struct alignas(T) Slot {
        std::byte bytes[sizeof(T)];
    };

    std::array<Slot, Capacity> storage_{};
    std::array<T*, Capacity> free_list_{};
    std::size_t free_count_{0};
};

}  // namespace ontrade::memory
