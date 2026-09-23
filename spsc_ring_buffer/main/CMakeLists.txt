idf_component_register(
    SRCS "spsc_ring_buffer.cc"
    INCLUDE_DIRS "."
    PRIV_REQUIRES esp_driver_uart esp_timer
)

# Require C++20: we use std::atomic_ref, std::span.
target_compile_features(${COMPONENT_LIB} PUBLIC cxx_std_20)