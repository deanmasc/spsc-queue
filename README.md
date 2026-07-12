# spsc-queue
A single-producer single-consumer (SPSC) queue passes data between exactly two threads without locks. Because ownership of each end is fixed, no mutex or compare-and-swap loop is needed, the queue is lock-free by construction. 
