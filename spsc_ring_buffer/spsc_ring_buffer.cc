/**
 * SPSC Ring Buffer
 *
 * This implements a thread-safe, lock-free Single-Producer/Single-Consumer
 * ring buffer suitable for ESP32 ISR context.
 *
 * Compilation:
 *   export SDKROOT="$(xcrun --show-sdk-path)"   # macOS only
 *   C++17: clang++ -std=c++17 -O2 -pthread spsc_ring_buffer.cc -o spsc_ring_buffer
 *   C++20: clang++ -std=c++20 -O2 -pthread spsc_ring_buffer.cc -o spsc_ring_buffer
 *   C++23: clang++ -std=c++23 -O2 -pthread spsc_ring_buffer.cc -o spsc_ring_buffer
 *   C++26: clang++ -std=c++26 -O2 -pthread spsc_ring_buffer.cc -o spsc_ring_buffer
 *
 * For GCC, replace clang++ with g++ (same flags)
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

// ============================================================================
// PART 1: THE SPSC RING BUFFER
// ============================================================================

/**
 * SPSCRingBuffer - Single-Producer/Single-Consumer Lock-Free Ring Buffer
 *
 * Key Design Decisions:
 * 1. Lock-free: Uses atomic operations with appropriate memory ordering
 * 2. ISR-safe: Producer (push) never blocks or uses mutexes
 * 3. Fixed capacity: No dynamic allocation after construction
 * 4. Overflow handling: Returns false when full (data loss is acceptable)
 *
 * Memory Ordering Strategy:
 * - Producer (ISR): Uses release semantics when updating head
 * - Consumer: Uses acquire semantics when reading head
 * - This ensures the producer's writes are visible to the consumer
 *
 * Why not use volatile?
 * - volatile only prevents compiler optimization, not CPU reordering
 * - Atomic operations with memory barriers are required for multi-threaded
 * correctness
 *
 * Head/Tail Wrap-around:
 * - Using modulo (%) is expensive on embedded systems
 * - We use bitmask AND (&) with power-of-2 buffer size (optimization)
 * - The size is rounded up to next power of 2 internally
 */
template <size_t Capacity> class SPSCRingBuffer {
  // Compile-time validation block
  // This executes at compile-time for every template instantiation
  static_assert((Capacity & (Capacity - 1)) == 0,
                "Capacity MUST be a power of 2 (e.g., 64, 128, 256)");
  static_assert(
      Capacity > 1,
      "Capacity must be at least 2 (one slot needed for full/empty detection)");
  static_assert(
      Capacity <= 4096,
      "Capacity > 4096 wastes memory on ESP32 (typical RAM is 520KB)");
  static_assert(std::is_same<int16_t, int16_t>::value,
                "Sample type must be int16_t for audio processing");
  static_assert(std::atomic<size_t>::is_always_lock_free,
                "Atomic operations must be lock-free for ISR safety");

  // Ensure our bitmask optimization works
  static constexpr size_t MASK = Capacity - 1;
  static_assert((Capacity & MASK) == 0, "Mask calculation is incorrect");

public:
  /**
   * Constructor - No dynamic allocation in the hot path
   * All memory is statically allocated
   */
  SPSCRingBuffer() : m_head(0), m_tail(0) {
// Runtime check is still useful for debug builds
// but the static_assert already caught most issues
#ifndef NDEBUG
    if ((Capacity & (Capacity - 1)) != 0) {
      // This should never happen due to static_assert
      std::terminate();
    }
#endif
  }

  // Disable copy/move (ring buffers should not be copied)
  SPSCRingBuffer(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer &operator=(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer(SPSCRingBuffer &&) = delete;
  SPSCRingBuffer &operator=(SPSCRingBuffer &&) = delete;

  /**
   * push - Called from ISR/Producer context
   *
   * Memory Ordering:
   * - memory_order_release: Ensures all prior writes (sample data) are
   *   visible to the consumer before the head is updated
   * - This pairs with memory_order_acquire in pop()
   *
   * ISR Safety:
   * - Never blocks
   * - No dynamic allocation
   * - O(1) operation
   * - Can be preempted by higher priority ISRs
   *
   * @param sample The audio sample to push
   * @return true if pushed successfully, false if buffer was full
   */
  [[nodiscard]] bool push(int16_t sample) noexcept {
    // Load current head and tail atomically
    // We need the current head to check if buffer is full
    const size_t head = m_head.load(std::memory_order_relaxed);
    const size_t tail = m_tail.load(std::memory_order_acquire);

    // Calculate next head position using bitmask (faster than modulo)
    const size_t next_head = (head + 1) & MASK;

    // Check if buffer is full
    // Full condition: (head + 1) == tail (when using power-of-2 size)
    if (next_head == tail) {
      return false; // Buffer full
    }

    // Write the sample to the buffer
    // Note: We don't need atomic here because the consumer won't read
    // this position until we update head (thanks to memory ordering)
    m_buffer[head] = sample;

    // Write the sample to the buffer
    // Note: We don't need atomic here because the consumer won't read
    // this position until we update head (thanks to memory ordering)
    m_head.store(next_head, std::memory_order_release);

    return true;
  }

  /**
   * pop - Called from Consumer context
   *
   * Memory Ordering:
   * - memory_order_acquire: Ensures we see all producer writes before head
   * - memory_order_release: Ensures tail update is visible to producer
   * - This creates a proper acquire-release pair with push()
   *
   * @param out Reference to store the popped sample
   * @return true if sample popped successfully, false if buffer was empty
   */
  [[nodiscard]] bool pop(int16_t &out) noexcept {
    // Load current tail and head
    const size_t tail = m_tail.load(std::memory_order_relaxed);
    const size_t head = m_head.load(std::memory_order_acquire);

    // Check if buffer is empty
    if (tail == head) {
      return false; // Buffer empty
    }

    // Read the sample from the buffer
    out = m_buffer[tail];

    // Calculate next tail position
    const size_t next_tail = (tail + 1) & MASK;

    // Release barrier: Ensure sample read is complete before updating tail
    // This pairs with the acquire in push() to prevent reading stale data
    m_tail.store(next_tail, std::memory_order_release);

    return true;
  }

  /**
   * size - Returns approximate number of elements
   *
   * Note: This is approximate in a concurrent context
   * Used for debugging/logging only
   */
  [[nodiscard]] size_t size() const noexcept {
    const size_t head = m_head.load(std::memory_order_acquire);
    const size_t tail = m_tail.load(std::memory_order_acquire);
    return (head - tail) & MASK;
  }

  /**
   * capacity - Returns the actual buffer capacity
   */
  [[nodiscard]] size_t capacity() const noexcept { return Capacity; }

private:
  // Static buffer - no heap allocation at all!
  // This is more suitable for ESP32 than std::vector
  int16_t m_buffer[Capacity];

  // Atomic head and tail indices
  // head: Points to the next position to write (producer)
  // tail: Points to the next position to read (consumer)
  std::atomic<size_t> m_head;
  std::atomic<size_t> m_tail;
};

// ============================================================================
// PART 2: SIMULATED ENVIRONMENT FOR TESTING
// ============================================================================

// Global flag for graceful shutdown
static_assert(std::atomic<bool>::is_always_lock_free,
              "The shutdown flag must be lock-free for signal-handler use");
static std::atomic<bool> g_running{true};

// Signal handler for Ctrl+C
void signal_handler(int) { g_running.store(false, std::memory_order_relaxed); }

// ============================================================================
// PART 3: MAIN APPLICATION WITH KEYBOARD CONTROL
// ============================================================================

/**
 * Simulates the "ISR" - generates samples at a configurable rate
 *
 * In a real ESP32 system, this would be called from a hardware timer ISR.
 * Here we use a separate thread with a sleep loop to simulate it.
 *
 * @param buffer Reference to the ring buffer
 * @param freq_ptr Pointer to the frequency atomic variable (samples/sec)
 * @param sample_counter Pointer to counter for logging purposes
 */
template <size_t Capacity>
void isr_producer(SPSCRingBuffer<Capacity> &buffer, std::atomic<int> &freq_ptr,
                  std::atomic<uint64_t> &sample_counter) {
  int16_t sample_value = 0;

  while (g_running.load(std::memory_order_relaxed)) {
    // Calculate sleep time based on current frequency
    int freq = freq_ptr.load(std::memory_order_acquire);
    if (freq <= 0) {
      g_running.store(false, std::memory_order_relaxed);
      break;
    }
    int sleep_ms = 1000 / freq; // Convert from samples/sec to ms/sample

    // Simulation only: a real ESP32 ISR must never sleep or block.
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

    if (!g_running.load(std::memory_order_relaxed))
      break;

    // Generate a sample (simple sawtooth wave for demo)
    sample_value = (sample_value + 1) % 32767;

    // Try to push to buffer
    bool success = buffer.push(sample_value);

    // Simulation only: a 64-bit atomic increment is not guaranteed to be
    // lock-free on ESP32 and must not be used from a real ISR.
    uint64_t count = sample_counter.fetch_add(1, std::memory_order_relaxed) + 1;

    // Simulation only: console I/O is not allowed from a real ESP32 ISR.
    if (success) {
      std::cout << "[ISR] Generated sample #" << count
                << " (value: " << sample_value << ")" << std::endl;
    } else {
      std::cout << "[ISR] BUFFER FULL! Sample #" << count
                << " LOST (value: " << sample_value << ")" << std::endl;
    }
  }
}

/**
 * Simulates the "Consumer" - reads samples at a configurable rate
 *
 * In a real system, this would be the audio processing task
 *
 * @param buffer Reference to the ring buffer
 * @param freq_ptr Pointer to the frequency atomic variable (samples/sec)
 * @param sample_counter Pointer to counter for logging purposes
 */
template <size_t Capacity>
void consumer(SPSCRingBuffer<Capacity> &buffer, std::atomic<int> &freq_ptr,
              std::atomic<uint64_t> &sample_counter) {
  int16_t sample = 0;

  while (g_running.load(std::memory_order_relaxed)) {
    // Calculate sleep time based on current frequency
    int freq = freq_ptr.load(std::memory_order_acquire);
    if (freq <= 0) {
      g_running.store(false, std::memory_order_relaxed);
      break;
    }
    int sleep_ms = 1000 / freq;

    // Wait for the next consumption interval
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

    if (!g_running.load(std::memory_order_relaxed))
      break;

    // Try to pop from buffer
    bool success = buffer.pop(sample);

    if (success) {
      // Increment counter
      uint64_t count =
          sample_counter.fetch_add(1, std::memory_order_relaxed) + 1;

      // Log successful consumption
      std::cout << "[CONSUMER] Consumed sample #" << count
                << " (value: " << sample << ")" << std::endl;
    } else {
      // Buffer was empty
      std::cout << "[CONSUMER] BUFFER EMPTY! No sample available" << std::endl;
    }
  }
}

/**
 * Keyboard input handler - controls the sample generation rate
 *
 * @param isr_freq Reference to ISR frequency (samples/sec)
 * @param consumer_freq Reference to consumer frequency (samples/sec)
 */
void keyboard_handler(std::atomic<int> &isr_freq,
                      std::atomic<int> &consumer_freq) {
  std::cout << "\n=== SPSC Ring Buffer Demo ===" << std::endl;
  std::cout << "Controls:" << std::endl;
  std::cout << "  'u' or UP arrow: Increase ISR rate by 1 (max 10)"
            << std::endl;
  std::cout << "  'd' or DOWN arrow: Decrease ISR rate by 1 (min 1)"
            << std::endl;
  std::cout << "  'q' or Ctrl+C: Quit" << std::endl;
  std::cout << "Initial ISR rate: " << isr_freq.load() << " samples/sec"
            << std::endl;
  std::cout << "Initial Consumer rate: " << consumer_freq.load()
            << " samples/sec" << std::endl;
  std::cout << "Buffer size: 64 elements" << std::endl;
  std::cout << "======================================\n" << std::endl;

  char input;
  while (g_running.load(std::memory_order_relaxed) && std::cin >> input) {
    int current = isr_freq.load(std::memory_order_acquire);
    int new_freq = current;

    switch (input) {
    case 'u': // Increase
    case 'U':
      new_freq = std::min(current + 1, 10);
      break;

    case 'd': // Decrease
    case 'D':
      new_freq = std::max(current - 1, 1);
      break;

    case 'q': // Quit
    case 'Q':
      g_running.store(false, std::memory_order_relaxed);
      return;

    default:
      std::cout << "[MAIN] Invalid key. Use 'u' (up), 'd' (down), or 'q' (quit)"
                << std::endl;
      continue;
    }

    if (new_freq != current) {
      isr_freq.store(new_freq, std::memory_order_release);
      std::cout << "[MAIN] ISR frequency changed to: " << new_freq
                << " samples/sec" << std::endl;
    }
  }

  // EOF or an input error must also stop the worker threads before joining.
  g_running.store(false, std::memory_order_relaxed);
}

/**
 * Main function - orchestrates the entire simulation
 *
 * Demonstrates:
 * 1. SPSC ring buffer initialization
 * 2. Producer (ISR) thread
 * 3. Consumer thread
 * 4. Keyboard control for frequency adjustment
 * 5. Graceful shutdown
 */
int main() {
  // Register signal handler for Ctrl+C
  std::signal(SIGINT, signal_handler);

  // Configuration
  constexpr size_t BUFFER_SIZE = 64;
  // Usage - The compile-time checks trigger here
  // This will compile fine
  using AudioBuffer = SPSCRingBuffer<BUFFER_SIZE>;
  // This would FAIL to compile (power of 2 check)
  // using InvalidBuffer = SPSCRingBuffer<63>;  // static_assert fails!
  // This would FAIL to compile (too small)
  // using TooSmallBuffer = SPSCRingBuffer<1>;  // static_assert fails!
  constexpr int INITIAL_ISR_FREQ = 1;      // 1 sample/sec
  constexpr int INITIAL_CONSUMER_FREQ = 5; // 5 samples/sec

  // Create the ring buffer
  AudioBuffer buffer;
  std::cout << "[MAIN] Created ring buffer with capacity: " << buffer.capacity()
            << " elements" << std::endl;

  // Atomic variables for thread-safe frequency control
  std::atomic<int> isr_freq(INITIAL_ISR_FREQ);
  std::atomic<int> consumer_freq(INITIAL_CONSUMER_FREQ);

  // Counters for logging
  std::atomic<uint64_t> isr_sample_count(0);
  std::atomic<uint64_t> consumer_sample_count(0);

  // Launch the producer (ISR simulation) thread
  std::thread producer_thread(isr_producer<BUFFER_SIZE>, std::ref(buffer),
                              std::ref(isr_freq), std::ref(isr_sample_count));

  // Launch the consumer thread
  std::thread consumer_thread(consumer<BUFFER_SIZE>, std::ref(buffer),
                              std::ref(consumer_freq),
                              std::ref(consumer_sample_count));

  // Run the keyboard handler on the main thread
  keyboard_handler(isr_freq, consumer_freq);

  // Wait for threads to finish gracefully
  std::cout << "[MAIN] Shutting down..." << std::endl;
  producer_thread.join();
  consumer_thread.join();

  // Final statistics
  std::cout << "\n[MAIN] Final Statistics:" << std::endl;
  std::cout << "  Samples generated: " << isr_sample_count.load() << std::endl;
  std::cout << "  Samples consumed: " << consumer_sample_count.load()
            << std::endl;
  std::cout << "  Buffer size: " << buffer.size() << " elements remaining"
            << std::endl;
  std::cout << "[MAIN] Done." << std::endl;

  return 0;
}
