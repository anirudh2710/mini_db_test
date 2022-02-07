#ifndef UTILS_NUMBERS_H
#define UTILS_NUMBERS_H

#include <limits>
#include <type_traits>
#include <cstdint>

#include <absl/strings/numbers.h>
#include <absl/strings/string_view.h>

namespace taco {

namespace utils_numbers_impl {

template<class IntType, class = void>
struct SimpleAtoiWrapperImpl{
    static inline bool call(absl::string_view str, IntType *out) {
        return absl::SimpleAtoi(str, out);
    }
};

template<class IntType>
struct SimpleAtoiWrapperImpl<IntType,
    typename std::enable_if<sizeof(IntType) <= 2>::type> {

    // keep in sync with the signed-ness check in absl
    typedef typename std::conditional<
        (IntType)(1) - (IntType)(2) < 0,
        std::int32_t,
        std::uint32_t>::type int32;

    static inline bool call(absl::string_view str, IntType *out) {
        int32 int32_out;
        if (!absl::SimpleAtoi(str, &int32_out)) {
            return false;
        }

        if (int32_out < std::numeric_limits<IntType>::min())
            return false;
        if (int32_out > std::numeric_limits<IntType>::max())
            return false;
        *out = (IntType) int32_out;
        return true;
    }
};

}   // namespace utils_numbers_impl

/*!
 * A wrapper version of absl::SimpleAtoi that supports 1/2/4/8-byte integers.
 */
template<class IntType>
inline bool SimpleAtoiWrapper(absl::string_view str, IntType *out) {
    return utils_numbers_impl::SimpleAtoiWrapperImpl<IntType>::call(str, out);
}


}   // namespace taco

#endif // UTILS_NUMBERS_H
