/**
 * SPSC Ring Buffer — ESP32 / Espressif QEMU edition
 *
 * Target: ESP-IDF v5.x (v5.1+ recommended), C++20.
 * Runs on:
 *   - Real ESP32 hardware
 *   - The official Espressif QEMU fork via `idf.py qemu monitor`
 *
 * Build & run:
 *   idf.py set-target esp32
 *   idf.py build
 *   idf.py qemu monitor           # or: idf.py -D QEMU=1 flash monitor
 *
 * Ports the PC version by replacing:
 *   std::thread              -> FreeRTOS tasks (xTaskCreatePinnedToCore)
 *   std::this_thread::sleep  -> vTaskDelay / vTaskDelayUntil
 *   std::cin / std::cout     -> UART console (stdin / ESP_LOGI)
 *   SIGINT                   -> 'q' key over the serial console
 *   std::chrono              -> esp_timer_get_time()
 *   std::expected (C++23)    -> sentinel values (no exceptions on embedded)
 */

#if __cplusplus < 202002L
#error "This example requires C++20 or later (std::atomic_ref, std::span)"
#endif

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>

#include "esp_log.h"
#include "esp_timer.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/uart.h"

// ============================================================================
// PART 1: THE SPSC RING BUFFER
// ============================================================================

/**
 * Result codes. We avoid std::expected (C++23) because:
 *   - The Xtensa/RISC-V toolchains that ship with ESP-IDF v5.x do not
 *     reliably support C++23 std::expected yet.
 *   - A single-byte enum is smaller and matches the "no allocation, no
 *     exception, no overhead" contract we want in an ISR-callable path.
 */
enum class PushResult : uint8_t { Ok, Full };
enum class PopResult  : uint8_t { Ok, Empty };

/**
 * SPSCRingBuffer — Single-Producer / Single-Consumer lock-free ring buffer.
 *
 * Design contract (unchanged from the PC version):
 *   - push() is safe to call from an ISR (no locks, no allocation, O(1)).
 *   - pop() is safe to call from a single consumer task.
 *   - Capacity must be a power of two.
 *   - One slot is sacrificed to distinguish full from empty.
 *
 * ESP32-specific considerations:
 *   - On ESP32, `std::atomic<size_t>` (32-bit) is always lock-free. This is
 *     enforced by static_assert below and is essential: any atomic that is
 *     not lock-free may take a global mutex, which cannot be used from ISR.
 *   - The producer runs on one core and the consumer on another (or the same
 *     core but different priority). Memory ordering via acquire/release pairs
 *     is what guarantees cross-core visibility without portMUX locks.
 *   - If your ISR must live in flash-safe code, add IRAM_ATTR to push().
 *     Here we keep it portable: vTaskDelay-driven producer instead of a
 *     hardware ISR, so IRAM_ATTR is unnecessary.
 */
template <size_t Capacity>
class SPSCRingBuffer {
  static_assert((Capacity & (Capacity - 1)) == 0,
                "Capacity MUST be a power of 2 (e.g., 64, 128, 256)");
  static_assert(Capacity >= 2,
                "Capacity must be at least 2 (one slot for full/empty)");
  static_assert(Capacity <= 4096,
                "Capacity > 4096 wastes SRAM on ESP32 (typical SRAM ~520KB)");
  static_assert(std::atomic<size_t>::is_always_lock_free,
                "atomic<size_t> must be lock-free for ISR safety on ESP32");
  static_assert(std::atomic_ref<size_t>::is_always_lock_free,
                "atomic_ref<size_t> must be lock-free for ISR safety");

  static constexpr size_t MASK = Capacity - 1;
  static_assert((Capacity & MASK) == 0, "Mask calculation is incorrect");

public:
  SPSCRingBuffer() = default;
  SPSCRingBuffer(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer &operator=(const SPSCRingBuffer &) = delete;
  SPSCRingBuffer(SPSCRingBuffer &&) = delete;
  SPSCRingBuffer &operator=(SPSCRingBuffer &&) = delete;

  /**
   * push — Producer side. ISR-callable.
   *
   * memory_order_release on m_head ensures the sample written to m_buffer
   * is visible to the consumer before the consumer sees the updated head.
   *
   * If you ever call this from a real hardware ISR:
   *   1. Add IRAM_ATTR to the function.
   *   2. Compile with CONFIG_ESP32_*_SPIRAM_* options that keep the buffer
   *      in internal SRAM (do not put m_buffer in PSRAM for DMA).
   */
  [[nodiscard]] PushResult push(int16_t sample) noexcept {
    const size_t head = m_head.load(std::memory_order_relaxed);
    const size_t tail = m_tail.load(std::memory_order_acquire);
    const size_t next_head = (head + 1) & MASK;

    if (next_head == tail) {
      return PushResult::Full;
    }

    m_buffer[head] = sample;
    m_head.store(next_head, std::memory_order_release);
    return PushResult::Ok;
  }

  /**
   * pop — Consumer side. Do NOT call from an ISR.
   *
   * memory_order_acquire on m_head pairs with the producer's release,
   * guaranteeing we see the sample before we advance m_tail.
   */
  [[nodiscard]] PopResult pop(int16_t &out) noexcept {
    const size_t tail = m_tail.load(std::memory_order_relaxed);
    const size_t head = m_head.load(std::memory_order_acquire);

    if (tail == head) {
      return PopResult::Empty;
    }

    out = m_buffer[tail];
    const size_t next_tail = (tail + 1) & MASK;
    m_tail.store(next_tail, std::memory_order_release);
    return PopResult::Ok;
  }

  [[nodiscard]] size_t size() const noexcept {
    const size_t head = m_head.load(std::memory_order_acquire);
    const size_t tail = m_tail.load(std::memory_order_acquire);
    return (head - tail) & MASK;
  }

  [[nodiscard]] static constexpr size_t capacity() noexcept { return Capacity; }

private:
  alignas(4) int16_t m_buffer[Capacity]{};  // 4-byte aligned for DMA friendliness
  alignas(4) std::atomic<size_t> m_head{0};
  alignas(4) std::atomic<size_t> m_tail{0};
};

// ============================================================================
// PART 2: GLOBAL STATE AND TASK-SHARED HANDLES
// ============================================================================

static const char *TAG_MAIN     = "MAIN";
static const char *TAG_ISR      = "ISR";
static const char *TAG_CONSUMER = "CONSUMER";

// Shutdown flag. On ESP32 we don't get SIGINT from a keyboard; the user
// presses 'q' on the serial console and the console task flips this flag.
static std::atomic<bool> g_running{true};

// Frequency controls, updated by the console task, read by the workers.
static std::atomic<int> g_isr_freq{1};       // samples / second
static std::atomic<int> g_consumer_freq{5};  // samples / second

// Logging counters.
static std::atomic<uint32_t> g_isr_count{0};
static std::atomic<uint32_t> g_consumer_count{0};

// Shared buffer. File scope is fine for a demo; in production, pass the
// reference into each task via the task argument (pvParameters).
static constexpr size_t kBufferSize = 64;
using AudioBuffer = SPSCRingBuffer<kBufferSize>;
static AudioBuffer g_buffer;

// ============================================================================
// PART 3: FREERTOS TASKS (the "threads" of the PC version)
// ============================================================================

/**
 * isr_producer_task — simulates the ISR cadence.
 *
 * In a real system, an esp_timer periodic callback (in ISR context) would
 * call g_buffer.push(). Here we use a high-priority task with vTaskDelay
 * so the demo is observable in QEMU without needing to wire a hardware timer.
 *
 * Task pinning: core 1, priority 10. Keeps Wi-Fi (core 0) undisturbed.
 * In a real design, audio pacing is a hardware timer ISR, not a task; the
 * push() call below is unchanged either way.
 */
static void isr_producer_task(void *) {
  ESP_LOGI(TAG_ISR, "producer task started (core %d)", xPortGetCoreID());

  int16_t sample_value = 0;
  TickType_t last_wake = xTaskGetTickCount();

  while (g_running.load(std::memory_order_relaxed)) {
    const int freq = g_isr_freq.load(std::memory_order_acquire);
    if (freq <= 0) {
      g_running.store(false, std::memory_order_relaxed);
      break;
    }

    // Interval in FreeRTOS ticks. With CONFIG_FREERTOS_HZ=1000 this is 1 ms.
    // For freq > 1000 you would need a hardware timer; for this demo the
    // range is 1..10 samples/sec, so ticks are perfectly fine.
    const TickType_t period = pdMS_TO_TICKS(1000 / freq);

    // vTaskDelayUntil keeps cadence drift-free (better than vTaskDelay).
    vTaskDelayUntil(&last_wake, period);
    if (!g_running.load(std::memory_order_relaxed)) break;

    sample_value = static_cast<int16_t>((sample_value + 1) % 32767);

    const PushResult r = g_buffer.push(sample_value);
    const uint32_t count =
        g_isr_count.fetch_add(1, std::memory_order_relaxed) + 1;

    if (r == PushResult::Ok) {
      ESP_LOGI(TAG_ISR, "generated #%u (value=%d)", count, sample_value);
    } else {
      // Data loss is acceptable — this is the buffer-full path.
      // We deliberately do NOT block or retry: the real-time cadence wins.
      ESP_LOGW(TAG_ISR, "BUFFER FULL — dropped #%u (value=%d)",
               count, sample_value);
    }
  }

  ESP_LOGI(TAG_ISR, "producer task exiting");
  vTaskDelete(nullptr);
}

/**
 * consumer_task — audio processing task.
 *
 * Pinned to core 1, priority 5 (lower than the producer). In a real design
 * this is where AEC, jitter buffer, codec, etc. would run.
 */
static void consumer_task(void *) {
  ESP_LOGI(TAG_CONSUMER, "consumer task started (core %d)", xPortGetCoreID());

  TickType_t last_wake = xTaskGetTickCount();
  int16_t sample = 0;

  while (g_running.load(std::memory_order_relaxed)) {
    const int freq = g_consumer_freq.load(std::memory_order_acquire);
    if (freq <= 0) {
      g_running.store(false, std::memory_order_relaxed);
      break;
    }

    const TickType_t period = pdMS_TO_TICKS(1000 / freq);
    vTaskDelayUntil(&last_wake, period);
    if (!g_running.load(std::memory_order_relaxed)) break;

    const PopResult r = g_buffer.pop(sample);

    if (r == PopResult::Ok) {
      const uint32_t count =
          g_consumer_count.fetch_add(1, std::memory_order_relaxed) + 1;
      ESP_LOGI(TAG_CONSUMER, "consumed #%u (value=%d)", count, sample);
    } else {
      ESP_LOGI(TAG_CONSUMER, "BUFFER EMPTY — nothing to consume");
    }
  }

  ESP_LOGI(TAG_CONSUMER, "consumer task exiting");
  vTaskDelete(nullptr);
}

// ============================================================================
// PART 4: CONSOLE (replaces std::cin / keyboard_handler)
// ============================================================================

/**
 * console_task — reads one byte at a time from UART0 via the UART driver.
 *
 * `idf.py monitor` (or QEMU's serial) is where you type. We use the raw UART
 * driver rather than getchar() because:
 *   - getchar() on ESP-IDF goes through a line-buffered path that doesn't
 *     behave well with single-keypress interaction.
 *   - The UART driver gives us a clean, non-blocking read with a timeout.
 *
 * Commands (one keypress, no Enter needed):
 *   u — increase producer rate (+1, capped at 10)
 *   d — decrease producer rate (-1, floored at 1)
 *   s — print status snapshot
 *   q — quit (stops workers, then idles)
 */
static void console_task(void *) {
  constexpr uart_port_t kPort = UART_NUM_0;

  // The default IDF console configures UART0; we only need a read buffer.
  // Reinstalling is harmless if it's already installed — we check first.
  if (!uart_is_driver_installed(kPort)) {
    uart_driver_install(kPort, 256, 0, 0, nullptr, 0);
  }

  ESP_LOGI(TAG_MAIN, "console ready — keys: u=up d=down s=status q=quit");

  while (g_running.load(std::memory_order_relaxed)) {
    uint8_t ch = 0;
    const int n = uart_read_bytes(kPort, &ch, 1, pdMS_TO_TICKS(200));
    if (n != 1) continue;  // timeout — loop and re-check g_running

    switch (ch) {
      case 'u': case 'U': {
        const int cur = g_isr_freq.load(std::memory_order_acquire);
        const int next = (cur < 10) ? cur + 1 : 10;
        if (next != cur) {
          g_isr_freq.store(next, std::memory_order_release);
          ESP_LOGI(TAG_MAIN, "ISR frequency -> %d samples/sec", next);
        }
        break;
      }
      case 'd': case 'D': {
        const int cur = g_isr_freq.load(std::memory_order_acquire);
        const int next = (cur > 1) ? cur - 1 : 1;
        if (next != cur) {
          g_isr_freq.store(next, std::memory_order_release);
          ESP_LOGI(TAG_MAIN, "ISR frequency -> %d samples/sec", next);
        }
        break;
      }
      case 's': case 'S': {
        ESP_LOGI(TAG_MAIN,
                 "status: isr_freq=%d/s consumer_freq=%d/s "
                 "buffered=%u generated=%u consumed=%u",
                 g_isr_freq.load(std::memory_order_acquire),
                 g_consumer_freq.load(std::memory_order_acquire),
                 static_cast<unsigned>(g_buffer.size()),
                 static_cast<unsigned>(g_isr_count.load(std::memory_order_relaxed)),
                 static_cast<unsigned>(g_consumer_count.load(std::memory_order_relaxed)));
        break;
      }
      case 'q': case 'Q':
        ESP_LOGI(TAG_MAIN, "quit requested");
        g_running.store(false, std::memory_order_relaxed);
        break;
      default:
        // Ignore CR/LF and unknown keys silently — the monitor sends \r\n.
        break;
    }
  }

  vTaskDelete(nullptr);
}

// ============================================================================
// PART 5: ENTRY POINT
// ============================================================================

/**
 * app_main — the ESP-IDF equivalent of main().
 *
 * Called by the IDF startup code after FreeRTOS is up. Returns and lets the
 * scheduler run — do NOT block here forever, or the idle task never runs.
 */
extern "C" void app_main(void) {
  ESP_LOGI(TAG_MAIN, "SPSC ring buffer demo starting");
  ESP_LOGI(TAG_MAIN, "buffer capacity = %u samples",
           static_cast<unsigned>(AudioBuffer::capacity()));

  // Producer: core 1, priority 10 (highest of the three app tasks).
  // Consumer: core 1, priority 5.
  // Console : core 0, priority 4 — keeps the UART away from the audio core.
  //
  // Stack sizes: 4096 bytes is ample. Logging uses a few hundred bytes on
  // the calling task's stack, so leave headroom.
  BaseType_t ok = pdPASS;
  ok &= xTaskCreatePinnedToCore(isr_producer_task, "isr_prod", 4096,
                                nullptr, 10, nullptr, 1);
  ok &= xTaskCreatePinnedToCore(consumer_task,     "consumer", 4096,
                                nullptr,  5, nullptr, 1);
  ok &= xTaskCreatePinnedToCore(console_task,      "console",  4096,
                                nullptr,  4, nullptr, 0);
  if (ok != pdPASS) {
    ESP_LOGE(TAG_MAIN, "task creation failed");
    return;
  }

  // app_main returns. The FreeRTOS scheduler keeps the three tasks running.
  // `idf.py monitor` will keep printing logs until you type 'q' (or Ctrl-]).
}
