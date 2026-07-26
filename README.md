# spsc-queue
A single-producer single-consumer (SPSC) queue passes data between exactly two threads without locks. Because ownership of each end is fixed, no mutex or compare-and-swap loop is needed, the queue is lock-free by construction. 


## FAQs
• Why is this queue only safe for exactly one producer and one consumer? What breaks with two
producers?


• What memory ordering guarantees are required between the slot write and the index update, and why?
• What is false sharing, and what do your benchmark numbers show its cost to be on your machine?
• Why must capacity be a power of two?
• What does your round-trip latency number tell you about the cost of the atomic operations involved?
• What would need to change to support multiple producers?