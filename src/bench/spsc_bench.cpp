// Benchmark suite for SPSCQueue: throughput, round-trip latency, false-sharing
// cost, payload-size scaling, and burst latency.
//
// Build (see src/bench/README.md for details):
//   clang++ -std=c++17 -O3 -DNDEBUG -pthread src/bench/spsc_bench.cpp -o src/bench/spsc_bench
//
#include "../spsc-queue/spsc_queue.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <mach/thread_policy.h>
#include <pthread.h>
#endif

using clock_type = std::chrono::steady_clock;

namespace {

// ---------------------------------------------------------------------------
// Thread pinning (best effort). macOS does not expose real per-core affinity
// on Apple Silicon (THREAD_AFFINITY_POLICY is only a grouping hint there and
// is a no-op on arm64), so this mainly matters on Intel Macs / Linux. We also
// raise QoS to USER_INTERACTIVE so the scheduler prefers performance cores
// and avoids preempting the benchmark threads.
void pin_to_core([[maybe_unused]] int core_id) {
#if defined(__APPLE__)
    pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
    thread_affinity_policy_data_t policy{core_id};
    thread_port_t mach_thread = pthread_mach_thread_np(pthread_self());
    thread_policy_set(mach_thread, THREAD_AFFINITY_POLICY,
                       reinterpret_cast<thread_policy_t>(&policy), THREAD_AFFINITY_POLICY_COUNT);
#endif
}

// ---------------------------------------------------------------------------
// Percentile helpers
double percentile(std::vector<double>& sorted_ns, double pct) {
    if (sorted_ns.empty()) return 0.0;
    double idx = (pct / 100.0) * static_cast<double>(sorted_ns.size() - 1);
    std::size_t lo = static_cast<std::size_t>(idx);
    std::size_t hi = std::min(lo + 1, sorted_ns.size() - 1);
    double frac = idx - static_cast<double>(lo);
    return sorted_ns[lo] + (sorted_ns[hi] - sorted_ns[lo]) * frac;
}

std::string format_ns(double ns) {
    char buf[64];
    if (ns < 1000.0) std::snprintf(buf, sizeof(buf), "%.1f ns", ns);
    else if (ns < 1'000'000.0) std::snprintf(buf, sizeof(buf), "%.2f us", ns / 1'000.0);
    else std::snprintf(buf, sizeof(buf), "%.2f ms", ns / 1'000'000.0);
    return buf;
}

void print_header(const char* title) {
    std::printf("\n=== %s ===\n", title);
}

// An unconstrained busy-poll that only ever touches cache-resident state the
// other core never invalidates can spin fast enough (>1e9 Hz) to starve the
// sibling core's real cross-core memory traffic -- a genuine effect on this
// hardware, not a logic bug. Yielding after a run of empty/full checks avoids
// it and is also just standard practice for a busy-poll loop.
class SpinBackoff {
    uint32_t streak_ = 0;

public:
    void spin() {
        if (++streak_ > 64) std::this_thread::yield();
    }
    void reset() { streak_ = 0; }
};

// ---------------------------------------------------------------------------
// 1) Throughput: max sustained message rate, producer/consumer on own threads.
template <uint32_t Capacity>
double run_throughput(std::chrono::milliseconds duration) {
    SPSCQueue<uint64_t, Capacity> q;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> produced{0};
    std::atomic<uint64_t> consumed{0};

    std::thread producer([&] {
        pin_to_core(0);
        uint64_t i = 0;
        SpinBackoff backoff;
        while (!stop.load(std::memory_order_relaxed)) {
            if (q.push(i)) {
                ++i;
                produced.store(i, std::memory_order_relaxed);
                backoff.reset();
            } else {
                backoff.spin();
            }
        }
    });

    std::thread consumer([&] {
        pin_to_core(1);
        uint64_t count = 0;
        SpinBackoff backoff;
        while (!stop.load(std::memory_order_relaxed)) {
            if (q.get().has_value()) {
                ++count;
                consumed.store(count, std::memory_order_relaxed);
                backoff.reset();
            } else {
                backoff.spin();
            }
        }
        // Drain remaining items after stop so consumed reflects real work done.
        while (q.get().has_value()) ++count;
        consumed.store(count, std::memory_order_relaxed);
    });

    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);
    producer.join();
    consumer.join();

    double seconds = duration.count() / 1000.0;
    return static_cast<double>(consumed.load()) / seconds;
}

// ---------------------------------------------------------------------------
// 2) Round-trip latency: producer stamps send time, consumer measures delta.
template <uint32_t Capacity>
std::vector<double> run_latency_samples(std::size_t num_samples) {
    SPSCQueue<uint64_t, Capacity> q;
    std::vector<double> latencies_ns;
    latencies_ns.reserve(num_samples);
    std::atomic<bool> done{false};

    std::thread consumer([&] {
        pin_to_core(1);
        // No backoff here deliberately: the producer paces itself (50us sleep
        // between sends), so empty gaps are brief and bounded -- there's no
        // sustained saturated contention to trigger the starvation effect
        // SpinBackoff exists for, and a real yield() would only add latency.
        while (latencies_ns.size() < num_samples) {
            auto v = q.get();
            if (!v.has_value()) continue;
            auto now = clock_type::now().time_since_epoch().count();
            latencies_ns.push_back(static_cast<double>(now - static_cast<int64_t>(*v)));
        }
        done.store(true, std::memory_order_relaxed);
    });

    pin_to_core(0);
    for (std::size_t i = 0; i < num_samples; ++i) {
        uint64_t send_ts = static_cast<uint64_t>(clock_type::now().time_since_epoch().count());
        while (!q.push(send_ts)) {
            // queue momentarily full; retry (shouldn't happen much: small samples, big capacity)
        }
        // Pace at a steady rate so we measure "typical" round trip, not burst behavior.
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    consumer.join();
    return latencies_ns;
}

void report_latency(const char* label, std::vector<double> latencies_ns) {
    std::sort(latencies_ns.begin(), latencies_ns.end());
    std::printf("%-28s p50=%-10s p99=%-10s p99.9=%-10s (n=%zu)\n", label,
                format_ns(percentile(latencies_ns, 50.0)).c_str(),
                format_ns(percentile(latencies_ns, 99.0)).c_str(),
                format_ns(percentile(latencies_ns, 99.9)).c_str(), latencies_ns.size());
}

// ---------------------------------------------------------------------------
// 3) False-sharing cost: compare cache-line-separated head/tail (SPSCQueue)
// against a "packed" variant where head and tail live on the same line.
template <typename T, uint32_t C>
class PackedSPSCQueue {
    static_assert(C != 0 && (C & (C - 1)) == 0, "Capacity must be a power of 2.");

    std::array<T, C> ring_buffer;
    // Deliberately NOT cache-line separated: head and tail sit next to each
    // other (and next to the buffer) so producer and consumer thrash the
    // same cache line on every push/get.
    std::atomic<uint32_t> head{};
    std::atomic<uint32_t> tail{};

    uint32_t get_index(uint32_t idx) const { return idx & (C - 1); }
    uint32_t size(uint32_t h, uint32_t t) const { return t - h; }
    bool is_empty(uint32_t h, uint32_t t) const { return size(h, t) == 0; }
    bool is_full(uint32_t h, uint32_t t) const { return size(h, t) == C; }

public:
    bool push(const T& val) {
        auto curr_head = head.load(std::memory_order_acquire);
        auto curr_tail = tail.load(std::memory_order_relaxed);
        if (is_full(curr_head, curr_tail)) return false;
        ring_buffer[get_index(curr_tail)] = val;
        tail.store(curr_tail + 1, std::memory_order_release);
        return true;
    }

    std::optional<T> get() {
        auto curr_head = head.load(std::memory_order_relaxed);
        auto curr_tail = tail.load(std::memory_order_acquire);
        if (is_empty(curr_head, curr_tail)) return std::nullopt;
        auto result = std::move(ring_buffer[get_index(curr_head)]);
        head.store(curr_head + 1, std::memory_order_release);
        return result;
    }

    uint32_t size() const {
        auto curr_head = head.load(std::memory_order_acquire);
        auto curr_tail = tail.load(std::memory_order_acquire);
        return curr_tail - curr_head;
    }
};

// ---------------------------------------------------------------------------
// 4) Payload size scaling: throughput as payload grows from 8 to 256 bytes.
template <std::size_t N>
struct Payload {
    static_assert(N >= 8, "Payload must be at least 8 bytes");
    std::array<std::byte, N> bytes{};
};

template <std::size_t N, uint32_t Capacity>
double run_payload_throughput(std::chrono::milliseconds duration) {
    // Heap-allocate: Capacity * sizeof(Payload<N>) can exceed the stack (e.g.
    // 65536 * 256B = 16MB), which a local SPSCQueue would silently overflow.
    auto q = std::make_unique<SPSCQueue<Payload<N>, Capacity>>();
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> consumed{0};
    Payload<N> sample{};

    std::thread producer([&] {
        pin_to_core(0);
        SpinBackoff backoff;
        while (!stop.load(std::memory_order_relaxed)) {
            if (q->push(sample)) backoff.reset();
            else backoff.spin();
        }
    });

    std::thread consumer([&] {
        pin_to_core(1);
        uint64_t count = 0;
        SpinBackoff backoff;
        while (!stop.load(std::memory_order_relaxed)) {
            if (q->get().has_value()) {
                ++count;
                backoff.reset();
            } else {
                backoff.spin();
            }
        }
        while (q->get().has_value()) ++count;
        consumed.store(count, std::memory_order_relaxed);
    });

    std::this_thread::sleep_for(duration);
    stop.store(true, std::memory_order_relaxed);
    producer.join();
    consumer.join();

    return static_cast<double>(consumed.load()) / (duration.count() / 1000.0);
}

// ---------------------------------------------------------------------------
// 5) Burst latency: producer sends bursts back-to-back, then idles.
template <uint32_t Capacity>
std::vector<double> run_burst_latency(std::size_t burst_size, std::size_t num_bursts,
                                       std::chrono::milliseconds gap) {
    static_assert(Capacity >= 1, "");
    SPSCQueue<uint64_t, Capacity> q;
    std::vector<double> latencies_ns;
    latencies_ns.reserve(burst_size * num_bursts);
    std::size_t expected = burst_size * num_bursts;

    std::thread consumer([&] {
        pin_to_core(1);
        // No backoff here either -- same reasoning as run_latency_samples.
        while (latencies_ns.size() < expected) {
            auto v = q.get();
            if (!v.has_value()) continue;
            auto now = clock_type::now().time_since_epoch().count();
            latencies_ns.push_back(static_cast<double>(now - static_cast<int64_t>(*v)));
        }
    });

    pin_to_core(0);
    for (std::size_t b = 0; b < num_bursts; ++b) {
        for (std::size_t i = 0; i < burst_size; ++i) {
            uint64_t send_ts = static_cast<uint64_t>(clock_type::now().time_since_epoch().count());
            while (!q.push(send_ts)) {
            }
        }
        std::this_thread::sleep_for(gap);
    }
    consumer.join();
    return latencies_ns;
}

// ---------------------------------------------------------------------------
// Used by benchmark 3 (false-sharing cost): forces the SAME moderate
// occupancy band onto every config via an external depth governor (checking
// size(), which does plain acquire loads with no interaction with the
// push/get code path under test), plus modest per-item work on both sides.
//
// A raw max-speed race (an earlier version of this benchmark) lets each
// config settle into whatever occupancy equilibrium its own internal costs
// happen to produce -- and different configs can settle at wildly different
// equilibria (one sitting near-empty, another near-full) even under
// identical nominal conditions, which then dominates the result for reasons
// that have nothing to do with false sharing. This holds occupancy fixed and
// identical across configs instead, so throughput differences can only come
// from the actual code path.
template <template <typename, uint32_t> class Queue, uint32_t Capacity>
double run_governed_throughput(std::chrono::milliseconds duration, uint32_t target_low,
                                uint32_t target_high, int per_item_work,
                                std::vector<uint32_t>* depth_samples = nullptr) {
    Queue<uint64_t, Capacity> q;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> consumed{0};
    volatile uint64_t sink = 0;

    std::thread producer([&] {
        pin_to_core(0);
        uint64_t i = 0;
        SpinBackoff gov, push_backoff;
        while (!stop.load(std::memory_order_relaxed)) {
            while (q.size() >= target_high && !stop.load(std::memory_order_relaxed)) gov.spin();
            if (stop.load(std::memory_order_relaxed)) break;
            for (int k = 0; k < per_item_work; ++k) sink += k;
            if (q.push(i)) {
                ++i;
                push_backoff.reset();
            } else {
                push_backoff.spin();
            }
        }
    });

    std::thread consumer([&] {
        pin_to_core(1);
        uint64_t count = 0;
        SpinBackoff gov, pop_backoff;
        while (!stop.load(std::memory_order_relaxed)) {
            while (q.size() <= target_low && !stop.load(std::memory_order_relaxed)) gov.spin();
            if (stop.load(std::memory_order_relaxed)) break;
            if (q.get().has_value()) {
                ++count;
                pop_backoff.reset();
                for (int k = 0; k < per_item_work; ++k) sink += k;
            } else {
                pop_backoff.spin();
            }
        }
        while (q.get().has_value()) ++count;
        consumed.store(count, std::memory_order_relaxed);
    });

    if (depth_samples) {
        auto start = clock_type::now();
        while (clock_type::now() - start < duration) depth_samples->push_back(q.size());
    } else {
        std::this_thread::sleep_for(duration);
    }
    stop.store(true, std::memory_order_relaxed);
    producer.join();
    consumer.join();

    return static_cast<double>(consumed.load()) / (duration.count() / 1000.0);
}

// Repeat a throughput measurement and take the median. A single 1s sample can
// land on a performance core one time and an efficiency core the next (real
// affinity isn't available on Apple Silicon), which swings raw throughput by
// 5-10x independent of anything the queue implementation does. The median
// across several trials filters most of that scheduler-placement noise out.
double median_of_trials(const std::function<double()>& trial, int num_trials) {
    std::vector<double> results;
    results.reserve(num_trials);
    for (int i = 0; i < num_trials; ++i) results.push_back(trial());
    std::sort(results.begin(), results.end());
    return results[results.size() / 2];
}

} // namespace

int main() {
    constexpr uint32_t kCapacity = 1u << 16;
    constexpr auto kTrialDuration = std::chrono::milliseconds(300);
    constexpr int kNumTrials = 9;

    // 1) Throughput
    print_header("1) Throughput (dedicated producer/consumer threads, median of 9 trials)");
    double msgs_per_sec = median_of_trials(
        [&] { return run_throughput<kCapacity>(kTrialDuration); }, kNumTrials);
    std::printf("%.2f M msgs/sec (%.0f msgs/sec)\n", msgs_per_sec / 1e6, msgs_per_sec);

    // 2) Round-trip latency
    print_header("2) Round-trip latency (steady rate, ~20k msgs/sec)");
    auto latency_samples = run_latency_samples<4096>(20'000);
    report_latency("steady-rate", std::move(latency_samples));

    // 3) False-sharing cost (cache-line separated vs packed head/tail), under
    // a governed occupancy band rather than a raw max-speed race. A max-speed
    // race lets each config settle into whatever occupancy equilibrium its
    // own internal costs happen to produce -- and different configs can land
    // at wildly different equilibria (one near-empty, another near-full) even
    // under identical nominal conditions, which then dominates the result for
    // reasons that have nothing to do with false sharing. Holding occupancy
    // fixed and identical across configs isolates the actual effect.
    print_header("3) False-sharing cost (governed occupancy band, rotated trials)");
    {
        constexpr uint32_t kLow = 2000, kHigh = 8000;
        constexpr int kWork = 2;

        auto occ = [&](const char* name, auto run) {
            std::vector<uint32_t> samples;
            run(samples);
            std::sort(samples.begin(), samples.end());
            std::printf("occupancy %-9s p10=%6u  p50=%6u  p90=%6u  max=%6u\n", name,
                        samples[samples.size() / 10], samples[samples.size() / 2],
                        samples[samples.size() * 9 / 10], samples.back());
        };
        occ("separated", [&](std::vector<uint32_t>& s) {
            run_governed_throughput<SPSCQueue, kCapacity>(kTrialDuration, kLow, kHigh, kWork, &s);
        });
        occ("packed", [&](std::vector<uint32_t>& s) {
            run_governed_throughput<PackedSPSCQueue, kCapacity>(kTrialDuration, kLow, kHigh, kWork,
                                                                 &s);
        });

        // Rotate run order across trials so neither config systematically
        // benefits from a fixed warm-up/cool-down slot.
        std::vector<double> sep, pkd;
        for (int i = 0; i < kNumTrials; ++i) {
            double s, p;
            if (i % 2 == 0) {
                s = run_governed_throughput<SPSCQueue, kCapacity>(kTrialDuration, kLow, kHigh, kWork);
                p = run_governed_throughput<PackedSPSCQueue, kCapacity>(kTrialDuration, kLow, kHigh,
                                                                        kWork);
            } else {
                p = run_governed_throughput<PackedSPSCQueue, kCapacity>(kTrialDuration, kLow, kHigh,
                                                                        kWork);
                s = run_governed_throughput<SPSCQueue, kCapacity>(kTrialDuration, kLow, kHigh, kWork);
            }
            sep.push_back(s);
            pkd.push_back(p);
        }
        std::sort(sep.begin(), sep.end());
        std::sort(pkd.begin(), pkd.end());
        std::printf("separated: median=%.2f M/s  p10=%.2f  p90=%.2f\n", sep[sep.size() / 2] / 1e6,
                    sep[sep.size() / 10] / 1e6, sep[sep.size() * 9 / 10] / 1e6);
        std::printf("packed:    median=%.2f M/s  p10=%.2f  p90=%.2f\n", pkd[pkd.size() / 2] / 1e6,
                    pkd[pkd.size() / 10] / 1e6, pkd[pkd.size() * 9 / 10] / 1e6);
    }

    // 4) Payload size scaling
    print_header("4) Payload size scaling (8 -> 256 bytes, median of 9 trials)");
    {
        auto report = [&](std::size_t bytes, double rate) {
            std::printf("%4zu B: %8.2f M msgs/sec  (%7.1f MB/sec)\n", bytes, rate / 1e6,
                        rate * static_cast<double>(bytes) / 1e6);
        };
        report(8, median_of_trials([&] { return run_payload_throughput<8, kCapacity>(kTrialDuration); }, kNumTrials));
        report(32, median_of_trials([&] { return run_payload_throughput<32, kCapacity>(kTrialDuration); }, kNumTrials));
        report(64, median_of_trials([&] { return run_payload_throughput<64, kCapacity>(kTrialDuration); }, kNumTrials));
        report(128, median_of_trials([&] { return run_payload_throughput<128, kCapacity>(kTrialDuration); }, kNumTrials));
        report(256, median_of_trials([&] { return run_payload_throughput<256, kCapacity>(kTrialDuration); }, kNumTrials));
    }

    // 5) Burst latency
    print_header("5) Burst latency (bursts of 64, 5ms gap between bursts)");
    auto burst_samples = run_burst_latency<4096>(64, 300, std::chrono::milliseconds(5));
    report_latency("burst", std::move(burst_samples));

    return 0;
}
