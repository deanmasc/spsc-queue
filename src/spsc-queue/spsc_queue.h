#ifndef SPSC_QUEUE_H
#define SPSC_QUEUE_H

#include <atomic>
#include <array>
#include <cstdlib>
#include <optional>
#include <iostream>

template<typename T, uint32_t C>
class SPSCQueue {
    static_assert(C != 0 && (C & (C - 1)) == 0, "Capacity must be a power of 2.");

    std::array<T, C> ring_buffer;
    // Both atomic to disallow race conditions
    std::atomic<uint32_t> head {};
    std::atomic<uint32_t> tail {};

    uint32_t get_index(const uint32_t& idx) const {
        // Use bit-mask with AND to get the remainder (C is always a power of 2)
        return idx & (C - 1); 
    }

public:
    bool push(T& val) {
        if (is_full()) return false;

        // This lines loads the head using memory_order_acquire, because we want to make sure the lines 
        // after it do not run before it, so we guarentee we get tail first, then increment it
        auto tail_idx {get_index(tail.load(std::memory_order_acquire))};
        ring_buffer[tail_idx] = val;
        // This line ensures that the writing of tail is indivisibleno line before the store is moved to 
        // after it, therefore once tail has been updated we know the operations before it have also ran
        tail.store(++tail, std::memory_order_release);
        return true;
    }

    bool push(T&& val) {
        if (is_full()) return false;

        // This lines loads the head using memory_order_acquire, because we want to make sure the lines 
        // after it do not run before it, so we guarentee we get tail first, then increment it
        auto tail_idx {get_index(tail.load(std::memory_order_acquire))};
        ring_buffer[tail_idx] = std::move(val);
        // This line ensures that the writing of tail is indivisibleno line before the store is moved to 
        // after it, therefore once tail has been updated we know the operations before it have also ran
        tail.store(++tail, std::memory_order_release);
        return true;
    }
    
    std::optional<T> get() {
        if (is_empty()) return std::nullopt;

        // This lines loads the head using memory_order_acquire, because we want to make sure the lines 
        // after it do not run before it, so we guarentee we get head first, then increment it
        auto head_idx {get_index(head.load(std::memory_order_acquire))};
        auto result{ring_buffer[head_idx]};
        // This line ensures that the writing of tail is indivisibleno line before the store is moved to 
        // after it, therefore once tail has been updated we know the operations before it have also ran
        head.store(++head, std::memory_order_release);
        return result;
    }

    uint32_t size() const {
        return tail.load(std::memory_order_acquire) - head.load(std::memory_order_acquire);
    }

    bool is_empty() const {
        return this->size() == 0;
    }

    bool is_full() const {
        return this->size() >= C;
    }

    void print() const {
        auto valid_head {head.load(std::memory_order_acquire)};
        auto valid_tail {tail.load(std::memory_order_acquire)};

        for (uint32_t i{valid_head}; i < valid_tail; i++) {
            std::cout << ring_buffer[get_index(i)] << ", ";
        }
        std::cout << "\n";
    }
};

#endif
