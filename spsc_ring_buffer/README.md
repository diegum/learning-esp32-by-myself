# SPSC Ring Buffer Demo

A C++ demonstration of a lock-free single-producer/single-consumer (SPSC) ring buffer. The program simulates an ESP32-style producer ISR with a thread and an audio-processing consumer with another thread.

## Features

- Lock-free producer and consumer operations using `std::atomic`
- Acquire/release memory ordering between producer and consumer
- Fixed-size storage allocated during construction
- Power-of-two capacity for efficient index wrapping
- Non-blocking behavior when the buffer is empty or full
- Interactive producer-rate control

## Requirements

- C++17 or newer
- Clang or GCC
- POSIX threads support

On macOS, Apple Clang requires the Xcode Command Line Tools or Xcode. The non-system `clang++` installed in this environment also needs `SDKROOT` set so it can find macOS system headers.

## Build

### macOS

```sh
export SDKROOT="$(xcrun --show-sdk-path)"
clang++ -std=c++26 -O2 -pthread spsc_ring_buffer.cpp -o spsc_ring_buffer
```

Alternatively, use Apple Clang directly:

```sh
/usr/bin/clang++ -std=c++26 -O2 -pthread spsc_ring_buffer.cpp -o spsc_ring_buffer
```

Any supported language mode can be used by replacing `c++26` with `c++17`, `c++20`, or `c++23`.

### GCC

```sh
g++ -std=c++20 -O2 -pthread spsc_ring_buffer.cpp -o spsc_ring_buffer
```

## Run

```sh
./spsc_ring_buffer
```

The program starts with a producer rate of 1 sample per second and a consumer rate of 5 samples per second.

Controls:

- `u` or `U`: increase the producer rate, up to 10 samples per second
- `d` or `D`: decrease the producer rate, down to 1 sample per second
- `q` or `Q`: stop the demo
- `Ctrl+C`: stop the demo through the signal handler

The output reports generated samples, consumed samples, buffer-full events, and buffer-empty events.

## Ring Buffer API

`SPSCRingBuffer` exposes:

- `push(int16_t sample)`: add a sample; returns `false` when full
- `pop(int16_t& out)`: remove a sample; returns `false` when empty
- `is_empty()`: check whether the buffer is empty
- `size()`: return the current approximate number of stored elements
- `capacity()`: return the allocated power-of-two capacity

The requested capacity is rounded up to the next power of two. Because the implementation uses one empty slot to distinguish full from empty, the usable number of elements is one less than the reported capacity.

## Design Notes

The buffer assumes exactly one producer and one consumer. It is not safe for multiple producers or multiple consumers without additional synchronization.

The producer publishes a written sample by storing the new head index with release ordering. The consumer loads that index with acquire ordering before reading the sample. The same pattern is used for the tail index so the producer can safely reuse consumed slots.

The demo's producer thread represents an ISR for educational purposes. The thread uses sleeping and console output, neither of which is appropriate inside a real hardware ISR.
