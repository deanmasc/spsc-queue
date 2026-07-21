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
    uint32_t head {};
    uint32_t tail {};

    uint32_t get_index(const uint32_t& idx) const {
        // Use bit-mask with AND to get the remainder (C is always a power of 2)
        return idx & (C - 1); 
    }

public:
    bool push(T& val) {
        if (is_full()) return false;

        ring_buffer[get_index(tail)] = val;
        ++tail;
        return true;
    }

    bool push(T&& val) {
        if (is_full()) return false;

        ring_buffer[get_index(tail)] = std::move(val);
        ++tail;
        return true;
    }
    
    std::optional<T> get() {
        if (is_empty()) return std::nullopt;

        auto result{ring_buffer[get_index(head)]};
        ++head;
        return result;
    }

    uint32_t size() const {
        return tail - head;
    }

    bool is_empty() const {
        return this->size() == 0;
    }

    bool is_full() const {
        return this->size() >= C;
    }

    void print() const {
        for (uint32_t i{head}; i < tail; i++) {
            std::cout << ring_buffer[get_index(i)] << ", ";
        }
        std::cout << "\n";
    }
};

#endif
