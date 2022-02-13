/*!
 * @file
 *
 * This file includes c++11 atomic library.
 *
 * This file does not have a header guard intentionally, and it must be
 * included indirectly from base/tdb_base.h.
 */
#include <atomic>

using std::atomic;
using std::atomic_bool;
using std::atomic_flag;
using std::atomic_char;
using std::atomic_short;
using std::atomic_ushort;
using std::atomic_long;
using std::atomic_ulong;
using std::atomic_llong;
using std::atomic_ullong;
using std::atomic_intptr_t;
using std::atomic_uintptr_t;
using std::atomic_size_t;
using std::atomic_ptrdiff_t;
using std::atomic_int8_t;
using std::atomic_uint8_t;
using std::atomic_int16_t;
using std::atomic_uint16_t;
using std::atomic_int32_t;
using std::atomic_uint32_t;
using std::atomic_int64_t;
using std::atomic_uint64_t;

using std::memory_order_relaxed;
using std::memory_order_consume;
using std::memory_order_release;
using std::memory_order_acquire;
using std::memory_order_acq_rel;
using std::memory_order_seq_cst;

