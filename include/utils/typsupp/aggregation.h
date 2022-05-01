#ifndef UTILS_TYPSUPP_AGGREGATION_H
#define UTILS_TYPSUPP_AGGREGATION_H

#include "tdb.h"

#include <type_traits>

namespace taco {

struct SumState {
    bool    m_empty;
    union {
        uint64_t m_unsigned;
        int64_t  m_signed;
        double   m_float;
    }       m_agg;
};

template<typename T, typename = void>
struct SumStateAggType;

template<typename T>
struct SumStateAggType<T, typename std::enable_if<
    std::is_integral<T>::value && std::is_signed<T>::value>::type> {
    typedef int64_t A;
};

template<typename T>
struct SumStateAggType<T, typename std::enable_if<
    std::is_integral<T>::value && !std::is_signed<T>::value>::type> {
    typedef uint64_t A;
};

template<typename T>
struct SumStateAggType<T, typename std::enable_if<
    std::is_floating_point<T>::value>::type> {
    typedef double A;
};

struct AvgState {
    double      m_sum;
    uint64_t    m_cnt;
};

struct PrimitiveMinMaxState {
    bool    m_empty;
    union {
        uint64_t u64;
        uint32_t u32;
        uint16_t u16;
        uint8_t u8;
        int64_t i64;
        int32_t i32;
        int16_t i16;
        int8_t i8;
        float f;
        double d;
    }   m_value;
};

}   // namespace taco

#endif  // UTILS_TYPSUPP_AGGREGATION_H
