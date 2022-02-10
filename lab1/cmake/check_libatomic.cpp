#include <atomic>

int main() {
    std::atomic_int x;
    x.store(5, std::memory_order_relaxed);
    int y = x.load(std::memory_order_relaxed);
    while (!x.compare_exchange_weak(y, 100, std::memory_order_seq_cst));
    return 0;
}
