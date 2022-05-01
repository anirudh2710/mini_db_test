#ifndef TDB_BASE_H
#define TDB_BASE_H
/*!
 * @file
 *
 * This file contains all the basic definitions in TDB. TDB is a C++11 project
 * and we try to avoid any non-standard code that relies on compiler
 * extensions, e.g., we try to avoid flexible array members if possible (which
 * are C99 compliant but not C++11 compliant unfortunately).
 */

#include "config.h"

// Always prefer the C++ new/delete expressions rather than using malloc except
// when one needs aligned_alloc(3) because aligned new is not available until
// C++17.
#ifdef USE_JEMALLOC
#   include <jemalloc/jemalloc.h>
#else
#   include <cstdlib>

using std::free;

// aligned_alloc is not in C++11 but should be available as long as
// _ISOC11_SOURCE is defined
#ifndef _ISOC11_SOURCE
#if __cplusplus >= 201703L
using std::aligned_alloc;
#else // __cplusplus < 201703L
#error "need a working aligned_alloc(3)"
#endif //  __cplusplus
#endif // _ISOC11_SOURCE

#endif // USE_JEMALLOC

#include <memory>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cctype>
#include <new>
#include <string>
#include <vector>
#include <utility>
#include <functional>
#include <limits>

#include "base/atomics.h"

#include <absl/base/macros.h>
#include <absl/memory/memory.h>
#include <absl/strings/string_view.h>

using std::size_t;
using std::ptrdiff_t;
using std::uint64_t;
using std::int64_t;
using std::uint32_t;
using std::int32_t;
using std::uint16_t;
using std::int16_t;
using std::int8_t;
using std::uint8_t;
using std::uintptr_t;
using std::intptr_t;

using std::static_pointer_cast;
using std::dynamic_pointer_cast;
using std::const_pointer_cast;
#if __cplusplus >= 201703L
// this is available starting from C++17
using std::reinterpret_pointer_cast;
#else
template<class T, class U>
static inline std::shared_ptr<T>
reinterpret_pointer_cast(const std::shared_ptr<U>& p) {
    return std::shared_ptr<T>(p, reinterpret_cast<T*>(p.get()));
}
#endif

namespace AlignedAllocImpl {
struct FreeMem {
    void operator()(void *x) {
        free(x);
    }
};
}   // AlignedAllocImpl

typedef std::unique_ptr<void, AlignedAllocImpl::FreeMem>
        unique_malloced_ptr;

/*!
 * Wraps an aligned_alloced'd memory space in a std::unique_ptr. It will be
 * deallocated using free(3) instead of delete expression.
 */
inline unique_malloced_ptr
unique_aligned_alloc(size_t alignment, size_t size) {
    return unique_malloced_ptr(aligned_alloc(alignment, size));
}

inline unique_malloced_ptr
wrap_malloc(void *ptr) {
    return unique_malloced_ptr(ptr);
}

inline unique_malloced_ptr
unique_malloc(size_t size) {
    return unique_malloced_ptr(malloc(size));
}

namespace AlignedAllocImpl {

template<size_t alignment, class T>
struct aligned_allocator {
    typedef T value_type;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    static T*
    allocate(size_t n) {
        return (T*) aligned_alloc(alignment, n * sizeof(T));
    }

    static void
    deallocate(T *p, size_t) {
        free(p);
    }

    constexpr bool
    operator==(const aligned_allocator&) {
        return true;
    }

    constexpr bool
    operator!=(const aligned_allocator&) {
        return false;
    }

    typedef std::true_type is_always_equal;

    template<class U>
    struct rebind {
        typedef aligned_allocator<alignment, U> other;
    };
};


}   // AlignedAllocImpl

using maxaligned_char_buf =
    std::vector<char, AlignedAllocImpl::aligned_allocator<8, char>>;


constexpr const size_t CACHELINE_SIZE = L1_CACHELINE_SIZE;
constexpr const size_t PAGE_SIZE = CONFIG_PAGE_SIZE;


#include "base/logging.h"
#include "utils/misc.h"

// ASSERT(condition [, <fmtstring> [, <args>]...])
//
// Assertion is enabled when NDEBUG is not defined.  Here we do not use the
// standard C assert() macro. Instead, it throws a fatal error so that our
// exception handler can catch it and provide better insight rather than an
// abort() call.
#if (defined(TDB_IN_TEST) && !defined(NDEBUG))
// In-test asserts shouldn't add the file location in the log, as they are
// always included in the log message.
#define ASSERT(...) \
    do { \
        bool __cond = (CAR(__VA_ARGS__)); \
        if (!__cond) { \
            LOG(::taco::kFatal, \
                    "assertion \"" \
                    STRINGIFY(CAR(__VA_ARGS__)) \
                    "\" failed" \
                    IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), ": ") \
                    CDR(__VA_ARGS__) \
                    ); \
        } \
    } while (0)
#elif (!defined(TDB_IN_TEST) && !defined(NDEBUG))
// Non-test assert failure should be a fatal error and needs to log a file
// location for debugging
#define ASSERT(...) \
    do { \
        bool __cond = (CAR(__VA_ARGS__)); \
        if (!__cond) { \
            LOG(::taco::kFatal, "%s:" STRINGIFY(__LINE__) \
                    ": assertion \"" \
                    STRINGIFY(CAR(__VA_ARGS__)) \
                    "\" failed" \
                    IF_NONEMPTY(CADR(__VA_ARGS__), ": "), \
                    ::taco::StripSourcePath(__FILE__) \
                    IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), ) \
                    CDR(__VA_ARGS__) \
                    ); \
        } \
    } while (0)
#else   // assert disabled
#define ASSERT(...)
#endif

namespace taco {

typedef uint32_t    Oid;
typedef int16_t     FieldOffset;
typedef int16_t     FieldId;
typedef uint32_t    PageNumber;
typedef uint64_t    BufferId;

/*!
 * The file ID. It is stored as tabfid in the systables. If the type of FileId
 * changes, the SQL type of tabfid must also change in
 * catalog/systables/Table.inc.
 */
typedef uint32_t    FileId;
typedef uint16_t    SlotId;

constexpr Oid InvalidOid = 0;
constexpr FieldId InvalidFieldId = 0x7fff;
constexpr FieldId MaxNumRegularFieldId = 0x7fff;
constexpr int PageNumberBits = 32;
constexpr int FieldOffsetBits = logn_ceil(PAGE_SIZE);
static_assert(FieldOffsetBits <= 15, "page size cannot be larger than 2^15");
constexpr FieldOffset MaxFieldOffset = (1 << FieldOffsetBits) - 1;

/*!
 * The invalid page number.
 */
constexpr PageNumber INVALID_PID = 0;

/*!
 * An invalid page number reserved for file manager internal use. It is never
 * visible outside the file manager.  \p INVALID_PID and all valid page numbers
 * are smaller than \p RESERVED_PID.
 */
constexpr PageNumber RESERVED_PID = ~(PageNumber) 0;

/*!
 * The maximum valid page number.
 */
constexpr PageNumber MaxPageNumber =
    (RESERVED_PID == ((((uint64_t) 1) << PageNumberBits) - 1))
        ? (RESERVED_PID - 2) : ((((uint64_t) 1) << PageNumberBits) - 1);

/*!
 * The invalid slot ID.
 */
constexpr SlotId INVALID_SID = 0;

/*!
 * The minimum valid slot ID.
 */
constexpr SlotId MinSlotId = 1;

/*!
 * The maximum valid slot ID. Makes sure adding one won't overflow.
 */
constexpr SlotId MaxSlotId = (~(SlotId) 0) - 1;

constexpr BufferId INVALID_BUFID = ~(BufferId) 0;

#define FIELDOFFSET_FORMAT "%hd"
#define FIELDID_FORMAT "%hd"
#define OID_FORMAT "%u"
#define FILEID_FORMAT "%u"
#define PAGENUMBER_FORMAT "0x%08X"
#define SLOTID_FORMAT "%hu"
#define BUFFERID_FORMAT "%lu"

/*!
 * The largest valid Oid. We make sure at least one Oid larger than this
 * is still a number that may be represented by the underlying type (so that
 * `MaxOid+ 1 > MaxOid` is always true).
 */
constexpr Oid MaxOid = std::numeric_limits<Oid>::max() - 1;

//! The index type, see index/idxtyps.h.
typedef uint8_t IdxType;

//! The operator type, see expr/optypes.h.
typedef uint8_t OpType;

/*!
 * The aggregation type, see catalog/aggtyp.h. All except the OTHER type may be
 * uniquely identified by the aggregation type and its operand type.  In all
 * cases, aggregations are also uniquely identified by operand type and its
 * name.
 */
typedef uint8_t AggType;

/*!
 * These alignment macros are derived from PostgreSQL. See COPYRIGHT for a
 * copyright notice on code for that. We assume we have 8-byte integers and
 * pointers so MAXALIGN_OF is fixed to 8. These macros are intentionally kept
 * as macros, so that we can use either some integral type or any flavor of
 * char* as the second argument, without writing tons of boiler-plate template
 * code with std::enable_if's. The downside is GDB can't invoke macros, but it
 * just takes too much effort to enable these in GDB.
 *
 * Alignment macros: align a length or address appropriately for a given type.
 * The fooALIGN() macros round up to a multiple of the required alignment,
 * while the fooALIGN_DOWN() macros round down.  The latter are more useful
 * for problems like "how many X-sized structures will fit in a page?".
 *
 * NOTE: TYPEALIGN[_DOWN] will not work if ALIGNVAL is not a power of 2.
 * That case seems extremely unlikely to be needed in practice, however.
 *
 * NOTE: MAXIMUM_ALIGNOF, and hence MAXALIGN(), intentionally exclude any
 * larger-than-8-byte types the compiler might have.
 */
template<class T>
constexpr T
TYPEALIGN(uint64_t ALIGNVAL, T LEN) {
    return (((uint64_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uint64_t) ((ALIGNVAL) - 1)));
}

#define SHORTALIGN(LEN)            TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)            TYPEALIGN(4, (LEN))
#define LONGALIGN(LEN)            TYPEALIGN(8, (LEN))
#define DOUBLEALIGN(LEN)        TYPEALIGN(8, (LEN))
#define MAXALIGN(LEN)            TYPEALIGN(8, (LEN))
#define CACHELINEALIGN(LEN)        TYPEALIGN(CACHELINE_SIZE, (LEN))
#define MAXALIGN_OF             8

template<class T>
constexpr T
TYPEALIGN_DOWN(uint64_t ALIGNVAL, T LEN) {
    return (((uint64_t) (LEN)) & ~((uint64_t) ((ALIGNVAL) - 1)));
}

#define SHORTALIGN_DOWN(LEN)    TYPEALIGN_DOWN(2, (LEN))
#define INTALIGN_DOWN(LEN)        TYPEALIGN_DOWN(4, (LEN))
#define LONGALIGN_DOWN(LEN)        TYPEALIGN_DOWN(8, (LEN))
#define DOUBLEALIGN_DOWN(LEN)    TYPEALIGN_DOWN(8, (LEN))
#define MAXALIGN_DOWN(LEN)        TYPEALIGN_DOWN(8, (LEN))
#define BUFFERALIGN_DOWN(LEN)    TYPEALIGN_DOWN(CACHELINE_SIZE, (LEN))

template<class T, class U>
bool
AddWithCheck(T &left, U right) {
    if ((left > 0 && (T) right > std::numeric_limits<T>::max() - left) ||
        (left < 0 && (T) right < left - std::numeric_limits<T>::min())) {
        return false;
    }
    left += (T) right;
    return true;
}

/*!
 * RETURN_IF(<cond> [, <optional_expr>]);
 *
 * <expr> is evaluated once. If the result is false, this macro expands into
 * a return statement with the optional expr as the return value.
 */
#define RETURN_IF(...) \
    do { \
        if ((CAR(__VA_ARGS__))) { \
            return CDR(__VA_ARGS__); \
        } \
    } while(0)

}       // namespace taco

/*!
 * Disallow FAM by declaring a negative-length array. The reason that macro
 * exists is that we want to make sure we don't accidentally include any FAM
 * from PostgreSQL source code.
 */
#define FLEXIBLE_ARRAY_MEMBER -1

#include "base/datum.h"
#include "base/fmgr.h"

#ifdef ABSL_USES_STD_STRING_VIEW
#define cast_as_string(t) t
#else

/*!
 * This is intended for casting anything that represents a string in C++ to an
 * std::string. Most of the cases use this forwarding implementation that
 * either passes an std::string as is or uses the implicit conversion to
 * convert something like a char* to an std::string. However, the
 * absl::string_view that is not using the c++17 std::string_view will use the
 * specialized version, which makes a copy of the content in the string_view.
 */
template<class T>
typename std::enable_if<!std::is_same<typename std::decay<T>::type,
                                      absl::string_view>::value,
                        T&&>::type
cast_as_string(T&& t) {
    return std::forward<T>(t);
}

template<class T>
typename std::enable_if<std::is_same<typename std::decay<T>::type,
                                     absl::string_view>::value,
                        std::string>::type
cast_as_string(T t) {
    return std::string(t.data(), t.size());
}

#endif // ABSL_USES_STD_STRING_VIEW

#endif  // TDB_BASE_H
