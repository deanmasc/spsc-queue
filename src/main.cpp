#include "spsc-queue/spsc_queue.h"
#include <iostream>

int main() {
    SPSCQueue<std::vector<int>, 4> q;
    std::vector<int> v1{1, 2, 3};
    q.push({4, 5, 6});
    q.push({7, 8, 9});
    std::cout << v1.size() << std::endl;
    q.push(std::move(v1));

    std::cout << v1.size() << std::endl;
    q.get();

    return 0;
}