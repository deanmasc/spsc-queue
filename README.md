# spsc-queue
A single-producer single-consumer (SPSC) queue passes data between exactly two threads without locks. Because ownership of each end is fixed, no mutex or compare-and-swap loop is needed, the queue is lock-free by construction. 


## FAQs
#### • Why is this queue only safe for exactly one producer and one consumer? What breaks with two producers?
- This queue is safe for one producer and consumer, because it guarentees that only one thread updates the 'head' variable and the same for the 'tail' variable
- We know this is true because the producer only updates tail upon insertion, and the consumer only updates head upon consumption
- If there were more than one producer or consumer this would cause data race issues and could only be resolved using locks, which would significantly hinder the performance of the queue.
- For example, imagine 2 threads try to push data to the queue at once, if they both read tail to be equal to N, then both threads will insert at that corresponding poition in the ring buffer and then increment tail to N + 1. This is a clear data race as tail should be N + 2, and there should be 2 new values in the ring buffer but the two threads race to update the same position. 

#### • What memory ordering guarantees are required between the slot write and the index update, and why?
- For both push and get operations its vital that the index update happens after the slot write, as this index update acts as an announcement to the other thread that there is that data in the ring buffer
- On the producer side, we need to make sure that the slot write always occurs before the tail update, and this ordering is maintained in terms of the updates to RAM. This is because the consumer is separate thread and does not have the guarentee the producer thread does of the ordering in the push operation as it doesnt have access to that cores cache and can fetch from RAM. If the tail variable is incremented first, and then the consumer tries to process a get() call, then there is a chance the consumer thinks there is data when there is not and this is clearly an issue.
- The same problem as above applies from the perspective of the consumer
- With the use of acquire and release ordering on both sides of the thread this guarentees that the thread that is acquiring data, gets the data at a state where it knows all the operations before it on the other threads side have run and that state is updated in the memory it is accessing.

#### • What is false sharing, and what do your benchmark numbers show its cost to be on your machine?
- False sharing is when we put data that different cores write to on the same cache line
- This is problematic because only one core can have modifyable access to a cache line at the same time, therefore if two threads want to modify variables in the same cache line the latter thread will have to wait until no other thread is modifying and then they will have to recieve the updated version from the core that just modified the cache line. And this competition for this cache line repeats and ping pongs
- In the case of this SPSC queue, we can avoid this problem because the producer core only ever updates the tail variable, and the consumer to the head variable. So, if we did put them on the same cache line then the two cores would constantly be waiting for modify access to the cache line, when they are writing to two different variables!
- Therefore, we can use alignment to ensure the tail and the head member varaibles ar eon different cache lines, therefore the producer never has to wait for the core to be done modifying a cache line the tail is on and vice versa.
- Benchmarking this performance difference is very hard because the results warp if the queue is empty a lot or full a lot. But attempting to control this results show a slight increase in throughput for separated cache lines at roughly 5-10% increase.

#### • Why must capacity be a power of two?
- To understand why this is done we need to explain what the head and tail member variables actually represent. Both of these variables are counters that get incremented each time an element is added and removed respectively. Therefore the values will not always be within the valid index range of the ring buffer
- Therefore we need a function that can convert these head and tail values to actual valid index values in the ring buffer when we need to push or get. We could just use the remainder operator, however this can be expensive. 
- However, if we know that the capacity of the queue is always a power of two, we can much more efficently get the real index (aka the remainder when dividing head/tail by capacity), we use the & operator with the idx and C-1 operands. This works because when doing C-1 on a power of two, it turns all the least significant bits to 1 (if C = 16, C-1 in bit-form is 00001111 converted from 00010000), therefore when using the & operator with the idx it gets only the least significant bits which give the remainder and the valid ring buffer indes in a much more efficient manner

#### • What does your round-trip latency number tell you about the cost of the atomic operations involved?
- results: p50=125.0 ns   p99=167.0 ns   p99.9=3.33 us    (n=20000)

#### • What would need to change to support multiple producers?
- Since having multiple producers introduces data races we need to ensure that only one thread can process the push functon at once, therefore here I would use a scoped_lock for a mutex for the functon, therefore if two producer threads call push at the same time, one has to wait until the second is finished.
- This would ensure that the two threads aren't reading and writing to the ring buffer and tail at the same time