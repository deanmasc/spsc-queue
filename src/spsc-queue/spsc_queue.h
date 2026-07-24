#ifndef SPSC_QUEUE_H
#define SPSC_QUEUE_H

#include <atomic>
#include <array>
#include <cstdint>
#include <optional>
#include <iostream>
#include <new>
#ifndef __cpp_lib_hardware_interference_size
    constexpr std::size_t hardware_destructive_interference_size = 64;
#else
    using std::hardware_destructive_interference_size;
#endif

template<typename T, uint32_t C>
class SPSCQueue {
    static_assert(C != 0 && (C & (C - 1)) == 0, "Capacity must be a power of 2.");

    alignas(hardware_destructive_interference_size) std::array<T, C> ring_buffer;
    alignas(hardware_destructive_interference_size) std::atomic<uint32_t> head {};
    alignas(hardware_destructive_interference_size) std::atomic<uint32_t> tail {};
    alignas(hardware_destructive_interference_size) std::atomic<uint32_t> head_cached_ {};

    uint32_t get_index(const uint32_t idx) const {
        // Use bit-mask with AND to get the remainder (C is always a power of 2)
        return idx & (C - 1); 
    }

    uint32_t size(const uint32_t head, const uint32_t tail) const {
        return tail - head;
    }

    bool is_empty(const uint32_t head, const uint32_t tail) const {
        return this->size(head, tail) == 0;
    }

    bool is_full(const uint32_t head, const uint32_t tail) const {
        return this->size(head, tail) == C;
    }

public:
    bool push(const T& val) {
        // We need to acquire the head first to ensure that any of the ahead operations
        // read stale values, the acquire also ensures that we read the updated state from the 
        // consumer, and that uses of this curr_head get the correct state
        // We also load curr_tail here to get a copy before we update it again
        auto curr_tail {tail.load(std::memory_order_relaxed)};

        // pass into is_full so we ensure we do not need to re-acquire
        if (is_full(head_cached_, curr_tail)) {
            head_cached = {head.load(std::memory_order_acquire)};
            if (is_full(head_cached_, curr_tail)) return false;
        };

        ring_buffer[get_index(curr_tail)] = val;
        // This line ensures that the writing of tail is indivisible, no line before the store is moved to 
        // after it, therefore once tail has been updated we know the operations before it have also ran
        // we are synchronising so that when the consumer acquired tail it will get the updated version
        tail.store(curr_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& val) {
        auto curr_tail {tail.load(std::memory_order_relaxed)};

        if (is_full(head_cached_, curr_tail)) {
            head_cached = {head.load(std::memory_order_acquire)};
            if (is_full(head_cached_, curr_tail)) return false;
        };

        ring_buffer[get_index(curr_tail)] = std::move(val);
        // This line ensures that the writing of tail is indivisible, no line before the store is moved to 
        // after it, therefore once tail has been updated we know the operations before it have also ran
        tail.store(curr_tail + 1, std::memory_order_release);
        return true;
    }
    
    std::optional<T> get() {
        auto curr_head {head.load(std::memory_order_relaxed)};
        auto curr_tail {tail.load(std::memory_order_acquire)};

        if (is_empty(curr_head, curr_tail)) return std::nullopt;

        auto result = std::move(ring_buffer[get_index(curr_head)]);
        // This line ensures that the writing of tail is indivisible, no line before the store is moved to 
        // after it, therefore once tail has been updated we know the operations before it have also ran
        head.store(curr_head + 1, std::memory_order_release);
        return result;
    }

    void print() const {
        auto valid_head {head.load(std::memory_order_acquire)};
        auto valid_tail {tail.load(std::memory_order_acquire)};

        for (uint32_t i{}; i < size(valid_head, valid_tail); i++) {
            std::cout << ring_buffer[get_index(i + valid_head)] << ", ";
        }
        std::cout << "\n";
    }

    uint32_t size() const {
        auto curr_head {head.load(std::memory_order_acquire)};
        auto curr_tail {tail.load(std::memory_order_acquire)};
        return curr_tail - curr_head;
    }
};

#endif
