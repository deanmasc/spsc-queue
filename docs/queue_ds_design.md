# Queue Data Structure

### Fields:
- `std::array<T, uint32_t C> ring_buffer`
- `uint32_t head`
- `uint32_t tail`

The head represents the index in the ring_buffer where the start of the queue is (oldest index), the tail represents the end of the queue (oldest index)

### Ring Buffer Implementation
- Since push and get have to be O(1) operations, we need to ensure for get calls we dont have to shift the array to ensure that the head is always at index 0. It would be much easier to manage the capacity of the queue is the head started at 0, however this would breach O(1) expectation.
- Therefore, we store head and tail and therefore form these values we can derive the start and end of the queue positions as well as the size.
- We know the capacity is full if head < tail then full if new_tail - head >= C, if head > tail then full if new_tail >= head.
- We can caclulate the size of the queue by: if head < tail then size is new_tail - head, if head > tail then size is C - head + tail
- We do not need to store a variable for C, because this is implied by ring_buffer.size()