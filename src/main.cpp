#include "spsc-queue/spsc_queue.h"
#include <iostream>

int main() {
    SPSCQueue<int, 4> q;
    int x {2};
    q.push(1);
    q.push(x);
    q.push(4);

    q.print();

    std::cout << *q.get() << std::endl;
    q.print();
    std::cout << *q.get() << std::endl;
    q.print();

    return 0;
}