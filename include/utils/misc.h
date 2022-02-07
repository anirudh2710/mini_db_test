#ifndef UTILS_MISC_H
#define UTILS_MISC_H


#define STRINGIFY_HELPER(_1) #_1
#define STRINGIFY(_1) STRINGIFY_HELPER(_1)

#define CONCAT_HELPER(_1, _2) _1 ## _2
#define CONCAT(_1, _2) CONCAT_HELPER(_1, _2)

#define CONCAT3_HELPER(_1, _2, _3) _1 ## _2 ## _3
#define CONCAT3(_1, _2, _3) CONCAT3_HELPER(_1, _2, _3)

// the ``garbage'' argument is required up until C++20 and for all C versions
#define SELECT_FIRST(...) SELECT_FIRST_HELPER(__VA_ARGS__, garbage)
#define SELECT_FIRST_HELPER(first, ...) first
#define SELECT_SECOND(...) SELECT_SECOND_HELPER(__VA_ARGS__, , garbage)
#define SELECT_SECOND_HELPER(first, second, ...) second

#define NOT(boolean_var) CONCAT(NOT_HELPER_, boolean_var)
#define NOT_HELPER_true false
#define NOT_HELPER_false true

// IF_NONEMPTY(arg_to_test, if_branch[, else_branch])
#define IF_NONEMPTY(arg, ...) \
    CONCAT(SELECT_, CONCAT(IF_NONEMPTY_HELPER_, IS_EMPTY(arg))) (__VA_ARGS__)
#define IF_NONEMPTY_HELPER_true SECOND
#define IF_NONEMPTY_HELPER_false FIRST

// IF_EMPTY(arg_to_test, if_branch[, else_branch])
#define IF_EMPTY(arg, ...) \
    CONCAT(SELECT_, CONCAT(IF_NONEMPTY_HELPER_, NOT(IS_EMPTY(arg)))) (__VA_ARGS__)

// add an optional comma if condition holds
#define COMMA_true ,
#define COMMA_false
#define IF_NONEMPTY_COMMA(arg, if_branch) \
    IF_NONEMPTY(arg, if_branch) CONCAT(COMMA_, NOT(IS_EMPTY(arg)))
#define IF_EMPTY_COMMA(arg, if_branch) \
    IF_EMPTY(arg, if_branch) CONCAT(COMMA_, IS_EMPTY(arg))

#define IF_BOOLEAN_LITERAL(arg, ...) \
    IF_EMPTY(CONCAT(IF_BOOLEAN_LITERAL_HELPER_, arg), __VA_ARGS__)
#define IF_BOOLEAN_LITERAL_HELPER_true
#define IF_BOOLEAN_LITERAL_HELPER_false

#define EXPAND_TO_COMMA(...) ,
#define HAS_COMMA_1_HELPER(_1, _2, _3, ...) _3
#define HAS_COMMA_1(...) HAS_COMMA_1_HELPER(__VA_ARGS__, t, f)
#define IS_EMPTY(arg) \
    CONCAT3(IS_EMPTY_HELPER_, \
    HAS_COMMA_1(EXPAND_TO_COMMA arg ()), \
    HAS_COMMA_1(EXPAND_TO_COMMA arg))
#define IS_EMPTY_HELPER_tf true
#define IS_EMPTY_HELPER_ff false
#define IS_EMPTY_HELPER_tt false

#define IS_NONEMPTY(arg) NOT(IS_EMPTY(arg))

#define HAS_ONLY_ONE___VA_ARGS__(...) \
    HAS_ONLY_ONE___VA_ARGS___HELPER(__VA_ARGS__,\
        f, f, f, f, f, f, f, f, f, f, \
        f, f, f, f, f, f, f, f, f, f, \
        f, f, f, f, f, f, f, f, f, f, \
        f, t, garbage)
#define HAS_ONLY_ONE___VA_ARGS___HELPER(\
   a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, \
   a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, \
   a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, \
   a31, a32, a33, ...) a33

// Contrary to lisp: we can call CAR and CDR on empty __VA_ARGS__
// CAR() and CDR() produce empty
// Limit of 33 arguments
#define CAR SELECT_FIRST
#define CDR(...) \
    CONCAT(CDR_HELPER_, HAS_ONLY_ONE___VA_ARGS__(__VA_ARGS__))(__VA_ARGS__)
#define CDR_HELPER_t(...)
#define CDR_HELPER_f(first, ...) __VA_ARGS__
#define CADR(...) CAR(CDR(__VA_ARGS__))
#define CADDR(...) CAR(CDR(CDR(__VA_ARGS__)))
#define CADDDR(...) CAR(CDR(CDR(CDR(__VA_ARGS__))))
#define CADDDDR(...) CAR(CDR(CDR(CDR(CDR(__VA_ARGS__)))))
#define CDDDDDR(...) CDR(CDR(CDR(CDR(CDR(__VA_ARGS__)))))

// Some of our catalog data generators also includes this file, but they don't
// want anything other than the macro definitions.
#ifndef INCLUDE_MACROS_ONLY
#include <utility>
#include <cstdint>
#include <absl/strings/string_view.h>

using std::uint64_t;

namespace taco {

template<class Container, class Arg0, class ...Args>
inline void
emplace_back_parameter_pack(Container &c, Arg0 &&arg0, Args&& ...args) {
    c.emplace_back(std::forward<Arg0>(arg0));
    emplace_back_parameter_pack(c, std::forward<Args>(args)...);
}

template<class Container>
inline void
emplace_back_parameter_pack(Container &c) {}

static constexpr char LogTable256[256] = {
#define LogTableLT(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n
   0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
   LogTableLT(4), LogTableLT(5), LogTableLT(5), LogTableLT(6),
   LogTableLT(6), LogTableLT(6), LogTableLT(6), LogTableLT(7),
   LogTableLT(7), LogTableLT(7), LogTableLT(7), LogTableLT(7),
   LogTableLT(7), LogTableLT(7), LogTableLT(7)
};
#undef LogTableLT

/*!
 * Returns $\lfloor log_2(x) \rfloor$ for x > 0, or 0 for x = 0.
 *
 * Ref: [Bit Twiddling Hacks](https://graphics.stanford.edu/~seander/bithacks.html#IntegerLogLookup}).
 */
constexpr int
logn_floor(uint64_t x) {
    return(x >> 32) ? (
            (x >> 48) ? (
                (x >> 56) ? (LogTable256[x >> 56] + 56)
                          : (LogTable256[x >> 48] + 48)
            ) : (
                (x >> 40) ? (LogTable256[x >> 40] + 40)
                          : (LogTable256[x >> 32] + 32)
            )
          ) : (
            (x >> 16) ?(
                (x >> 24) ? (LogTable256[x >> 24] + 24)
                          : (LogTable256[x >> 16] + 16)
            ) : (
                (x >> 8) ? (LogTable256[x >> 8] + 8)
                         : (LogTable256[x])
            )
          );
}

/*!
 * Returns $\lceil log_2(x) \rceil$ for x > 0, or 0 for x = 0.
 */
constexpr int
logn_ceil(uint64_t x) {
    return x ? (logn_floor(x - 1) + 1) : 0;
}

/*!
 * Returns true if filepath belongs to tdb source code path.
 *
 * Note that this function is used as a flags usage config function for absl
 * flags library, and we expect the \p filepath does not have the initial '/'.
 */
bool FilePathIsTDBFilePath(absl::string_view filepath);

absl::string_view StripSourcePath(absl::string_view path);

/*!
 * Returns the approximate size of heap-allocated memory in bytes.
 *
 * This returns stats.allocated if we're using jemalloc that was compiled with
 * --enable-stats (enabled by default). Otherwise, it returns the size of the
 *  data segment.
 */
uint64_t GetCurrentDataSize();

}   // namespace taco
#endif

#endif  // UTILS_MISC_H
