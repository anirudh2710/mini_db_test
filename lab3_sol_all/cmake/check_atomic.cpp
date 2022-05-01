/*!
 * Checks whether all integral and boolean std::atomic objects are always
 * lock-free and have no padding bits.
 */
#include <atomic>
#include <cstdint>
#include <thread>

static_assert(ATOMIC_BOOL_LOCK_FREE == 2);
static_assert(ATOMIC_CHAR_LOCK_FREE == 2);
static_assert(ATOMIC_SHORT_LOCK_FREE == 2);
static_assert(ATOMIC_INT_LOCK_FREE == 2);
static_assert(ATOMIC_LONG_LOCK_FREE == 2);
static_assert(ATOMIC_LLONG_LOCK_FREE == 2);
static_assert(ATOMIC_POINTER_LOCK_FREE == 2);

static_assert(sizeof(std::atomic_bool) == sizeof(bool));
static_assert(sizeof(std::atomic_char) == sizeof(char));
static_assert(sizeof(std::atomic_short) == sizeof(short));
static_assert(sizeof(std::atomic_int) == sizeof(int));
static_assert(sizeof(std::atomic_long) == sizeof(long));
static_assert(sizeof(std::atomic_llong) == sizeof(long long));
static_assert(sizeof(std::atomic_size_t) == sizeof(std::size_t));
static_assert(sizeof(std::atomic_ptrdiff_t) == sizeof(std::ptrdiff_t));
static_assert(sizeof(std::atomic_uintptr_t) == sizeof(std::uintptr_t));
static_assert(sizeof(std::atomic<void*>) == sizeof(void*));
static_assert(sizeof(std::atomic_int8_t) == sizeof(std::atomic_int8_t));
static_assert(sizeof(std::atomic_int16_t) == sizeof(std::atomic_int16_t));
static_assert(sizeof(std::atomic_int32_t) == sizeof(std::atomic_int32_t));
static_assert(sizeof(std::atomic_int64_t) == sizeof(std::atomic_int64_t));
static_assert(sizeof(std::atomic_uint8_t) == sizeof(std::atomic_uint8_t));
static_assert(sizeof(std::atomic_uint16_t) == sizeof(std::atomic_uint16_t));
static_assert(sizeof(std::atomic_uint32_t) == sizeof(std::atomic_uint32_t));
static_assert(sizeof(std::atomic_uint64_t) == sizeof(std::atomic_uint64_t));


int main() {
    std::thread f;
    return 0;
}
