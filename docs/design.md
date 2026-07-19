# SPSC Queue Design

### The Problem
- The problem with SPSC queue is not regarding reading and writing the same data, this is because the consumer and producer are writing to different sides of the queue, so this danger is averted for SPSC
- The core problem is in the case where the producer thread is mid writing to the tail of the queue (or the tail uint32_t), and the consumer reads the value partially written, therefore causing undefined behaviour, incorrect state of the queue or runtime errors 


### Solution
- As stated, the core issue is that the opposing thread reads from fields that may not be partially written yet, thereofre we need to ensure this is not possible
- We can ensure this by making the write a single instruction, to do this we can use std::atomic for the `ring_buffer`, `head` and `tail` variables.
- To do this we use atomic operations `load` and `store` with `std::memory_order_acquire` and `std::memory_order_release` 
- For writes we use `store` with `std::memory_order_release` so the write is done in a single isntruction and that no other statements in the code are moved from before the store to after the store. This is important because its ensuring that at the time of the write every line of code before it has run
- For reads we use `load` with `std::memory_order_acquire`, so that the read is done in a single instruction and no other statements in the code are moved from after the load to before the load. This is important because it ensures that any succeeding lines of code are run ensuring that the load was completed first
- Using `std::memory_order_release` and `std::memory_order_acquire` together becomes useful, because when we acquire from a data stucture that used release, we are now guarenteeing that the state we acquire the data it has has all the operations before the release synchronised and in the state
- Think of the case where on the producer side it does 2 key operations - it writes to the ring buffer, then increments the tail. There is a chance the comiler will reorder these statements so that the increment occurs before the writing to the ring buffer. If that occurs, then the consumer may see the queue has size of 1 for example, and try to read data, however the data is not yet written. By using release and acqire we ensure this reording does not occur.