#ifndef TESTS_BASE_TDB_TEST_H
#define TESTS_BASE_TDB_TEST_H

// Assertion macro ASSERT() is always enabled in tests by default, so that we
// can catch assertion failures as a fatal error. However, for performance
// tests, it might be desirable to disable the assertion, in which case one
// should define TDB_TEST_NDEBUG before including "tdb_test.h"
//#undef TDB_TEST_NDEBUG

#ifndef TDB_IN_TEST
#error "TDB_IN_TEST must be defined before including \"tdb.h\" in tests"
#endif
#include "tdb.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <random>

#include "utils/macro_map.h"

namespace taco {

// importing ::testing namespace for the matchers
using namespace ::testing;


// kInfo and kWarning are not error levels that are normally thrown as a
// TDBError exception. So we reuse them to indicate that the function does not
// throws any exception/throws a non-TDBError exception in the tests.
constexpr LogSeverity kNoError = kInfo;
constexpr LogSeverity kUnknownException = kWarning;

inline void
PrintTo(const TDBError &e, std::ostream* os) {
    *os << e.GetMessage();
}

template<class F>
TDBError R(F&& f) {
    try {
        f();
    } catch (const TDBError &e) {
        return e;
    } catch (const std::exception &e) {
        return TDBError(kUnknownException, e.what());
    } catch (...) {
        return TDBError(kUnknownException, "unknown exception caught");
    }
    return TDBError(kNoError, "Ok");
}

MATCHER(HasNoError, "") {
    return arg.GetSeverity() == kNoError;
}

MATCHER(HasRegularError, "") {
    return arg.GetSeverity() == kError;
}

MATCHER(HasFatalError, "") {
    return arg.GetSeverity() == kFatal;
}

MATCHER(HasAnyError, "") {
    return arg.GetSeverity() == kError || arg.GetSeverity() == kFatal;
}


#define SAFE_CAST_MATCHER_TO_TDBERROR_MATCHER(m) \
    SafeMatcherCast<TDBError>(m)
#define SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(...) \
    IF_EMPTY(CADR(__VA_ARGS__), \
        /* only one arg */ \
        SAFE_CAST_MATCHER_TO_TDBERROR_MATCHER(CAR(__VA_ARGS__)), \
        /* more than one arg */ \
        AllOf(MAP_LIST(SAFE_CAST_MATCHER_TO_TDBERROR_MATCHER, __VA_ARGS__)))


#define EXPECT_NO_ERROR(stmt) EXPECT_THAT(R([&](){stmt;}), HasNoError())
#define EXPECT_REGULAR_ERROR(...) \
    EXPECT_THAT(R([&](){CAR(__VA_ARGS__);}), \
        SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(\
            HasRegularError() \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), /* empty */) CDR(__VA_ARGS__)))
#define EXPECT_FATAL_ERROR(...) \
    EXPECT_THAT(R([&](){CAR(__VA_ARGS__);}), \
        SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(\
            HasFatalError() \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), /* empty */) CDR(__VA_ARGS__)))
#define EXPECT_ANY_ERROR(...) \
    EXPECT_THAT(R([&](){CAR(__VA_ARGS__);}), \
        SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(\
            HasAnyError() \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), /* empty */) CDR(__VA_ARGS__)))


#define ASSERT_NO_ERROR(stmt) ASSERT_THAT(R([&](){stmt;}), HasNoError())
#define ASSERT_REGULAR_ERROR(...) \
    ASSERT_THAT(R([&](){CAR(__VA_ARGS__);}), \
        SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(\
            HasRegularError() \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), /* empty */) CDR(__VA_ARGS__)))
#define ASSERT_FATAL_ERROR(...) \
    ASSERT_THAT(R([&](){CAR(__VA_ARGS__);}), \
        SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(\
            HasFatalError() \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), /* empty */) CDR(__VA_ARGS__)))
#define ASSERT_ANY_ERROR(...) \
    ASSERT_THAT(R([&](){CAR(__VA_ARGS__);}), \
        SAFE_CAST_ALL_MATCHERS_TO_TDBERROR_MATCHER(\
            HasAnyError() \
            IF_NONEMPTY_COMMA(CADR(__VA_ARGS__), /* empty */) CDR(__VA_ARGS__)))

/*!
 * SetUp assert failure won't stop the tests from running so one must throw
 * an exception in case of a set-up failure.
 */
class TDBTestSetUpFailure: public std::exception {
public:
    TDBTestSetUpFailure(std::string msg):
        m_msg(std::move(msg)) {}

    const char*
    what() const noexcept {
        return m_msg.c_str();
    }

private:
    std::string m_msg;
};

//! Enables the logging at kInfo level or above if it was disabled by TDBEnv.
void TestEnableLogging();

//! Restores the logging level as initially set in TDBEnv.
void TestDisableLogging();


/*!
 * Generates an alpha-numeric string of length between `[min_length,
 * max_length]` by first uniformly at random generate its length, and then
 * uniformly at random generate each of its characters in the set of
 * letters, digits, underscore and hyphen.
 */
template<class RNG>
std::string
GenerateRandomAlphaNumString(RNG &rng, size_t min_length, size_t max_length) {
    static const char ch[64] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
        'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',
        'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
        'y', 'z', '-', '_'
    };
    std::uniform_int_distribution<size_t> unif_len(min_length, max_length);
    size_t len = unif_len(rng);
    std::string str;
    str.reserve(len);

    std::uniform_int_distribution<uint8_t> unif_ch(0, 63);
    for (size_t i = 0; i < len; ++i) {
        uint8_t x = unif_len(rng);
        str.push_back(ch[x]);
    }
    return str;
}

}   // namespace taco

#endif  // TESTS_BASE_TDB_TEST_H
