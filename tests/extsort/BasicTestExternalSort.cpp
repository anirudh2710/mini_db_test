#include "base/TDBDBTest.h"

#include "extsort/ExternalSort.h"
#include "extsort/PseudoRandomItemIterator.h"

#include "utils/zerobuf.h"

ABSL_FLAG(uint64_t, test_sort_seed, 0xd2f8135abce88375ul,
          "The seed used in t he random number generator of the sort test.");
ABSL_FLAG(uint64_t, test_nitems, 100000,
          "The number of items to sort in the test.");
ABSL_FLAG(int64_t, test_nways, 8,
          "N way merges in the external sorting test.");
ABSL_FLAG(int64_t, test_int64_lb, -10000000l,
          "The lower bound of the randomly generated int64.");
ABSL_FLAG(int64_t, test_int64_ub, 10000000l,
          "The upper bound of the randomly generated int64.");
ABSL_FLAG(size_t, test_strlen_lb, 300,
          "The lower bound of the randomly generated int64.");
ABSL_FLAG(size_t, test_strlen_ub, 400,
          "The upper bound of the randomly generated int64.");

namespace taco {

template<class T,
         typename = typename std::enable_if<std::is_integral<T>::value>::type>
static int
s_int_cmp(const char *s1, const char *s2) {
    T lhs = *reinterpret_cast<const T*>(s1);
    T rhs = *reinterpret_cast<const T*>(s2);
    if (lhs == rhs)
        return 0;
    return (lhs < rhs) ? -1 : 1;
}

template<class T,
         typename = typename std::enable_if<std::is_integral<T>::value>::type>
static int
s_int_cmp_desc(const char *s1, const char *s2) {
    T lhs = *reinterpret_cast<const T*>(s1);
    T rhs = *reinterpret_cast<const T*>(s2);
    if (lhs == rhs)
        return 0;
    return (lhs < rhs) ? 1 : -1;
}

static SortCompareFunction s_str_cmp = std::strcmp;

static int
s_str_cmp_desc(const char *s1, const char *s2) {
    int res = std::strcmp(s1, s2);
    if (res == 0)
        return 0;
    return (res < 0) ? 1 : -1;
}

class BasicTestExternalSort: public TDBDBTest {
protected:
    void
    SetUp() override {
        // Disabling the buffer manager means we can't use anything in the
        // main file space, which is ok in sort tests since we're only using
        // temporary files.
        g_test_no_bufman = true;
        g_test_no_catcache = true;

        TDBDBTest::SetUp();
    }

    void
    TearDown() override {
        TDBDBTest::TearDown();
    }
};

template<class T, class = void>
struct SortResultMatchesComparator {};

template<class T>
struct SortResultMatchesComparator<T,
        typename std::enable_if<std::is_integral<T>::value>::type> {
    static
    bool Equals(ItemIterator *outiter, const T &expected) {
        FieldOffset len = 0;
        const char *pv = outiter->GetCurrentItem(len);
        if (((size_t) len) < sizeof(T) ||
            MAXALIGN((size_t) len) != MAXALIGN(sizeof(T))) {
            return false;
        }
        T actual = *reinterpret_cast<const T*>(pv);
        return actual == expected;
    }
};

template<>
struct SortResultMatchesComparator<std::string, void> {
    static
    bool Equals(ItemIterator *outiter, const std::string &expected) {
        FieldOffset len = 0;
        const char *pv = outiter->GetCurrentItem(len);
        if (((size_t) len) < expected.length() + 1 ||
            MAXALIGN((size_t) len) != MAXALIGN(expected.length() + 1)) {
            return false;
        }
        int res = strncmp(pv, expected.c_str(), expected.length());
        return res == 0;
    }
};

template<class T, class = void>
struct SortResultMatchesPrintItem {};

template<class T>
struct SortResultMatchesPrintItem<T,
        typename std::enable_if<std::is_integral<T>::value>::type> {
    static std::string
    Print(ItemIterator *outiter) {
        FieldOffset len = 0;
        const char *pv = outiter->GetCurrentItem(len);
        return absl::StrCat(*reinterpret_cast<const T*>(pv));
    }
};

template<>
struct SortResultMatchesPrintItem<std::string, void> {
    static std::string
    Print(ItemIterator *outiter) {
        FieldOffset len = 0;
        const char *pv = outiter->GetCurrentItem(len);
        return pv;
    }
};

template<class T>
static ::testing::AssertionResult
SortResultMatches(ItemIterator *outiter, const T* sorted, uint64_t n) {
    for (uint64_t i = 0; i < n; ++i) {
        if (!outiter->Next()) {
            return AssertionFailure() << "expecting " << n << " items "
                << "but only got " << i << " items";
        }
        if (!SortResultMatchesComparator<T>::Equals(outiter, sorted[i])) {
            return AssertionFailure() << "expecting " << (i+1)
                << "th item to be \""
                << sorted[i] << "\", but got \""
                << SortResultMatchesPrintItem<T>::Print(outiter)
                << "\"";
        }
    }
    return AssertionSuccess();
}


TEST_F(BasicTestExternalSort, TestSortUniformInt64Asc) {
    TDB_TEST_BEGIN

    const uint64_t seed = absl::GetFlag(FLAGS_test_sort_seed);
    const int64_t n = absl::GetFlag(FLAGS_test_nitems);
    const int64_t nways = absl::GetFlag(FLAGS_test_nways);
    const int64_t int64_lb = absl::GetFlag(FLAGS_test_int64_lb);
    const int64_t int64_ub = absl::GetFlag(FLAGS_test_int64_ub);
    PseudoRandomItemIterator<int64_t> iter(
        std::uniform_int_distribution<int64_t>(int64_lb, int64_ub),
        std::mt19937_64(seed), n);

    auto extsort = ExternalSort::Create(s_int_cmp<int64_t>, nways);
    std::unique_ptr<ItemIterator> outiter;
    ASSERT_NO_ERROR(outiter = extsort->Sort(&iter));

    const int64_t *sorted;
    ASSERT_NO_ERROR(sorted = iter.GetSortedItems(std::less<int64_t>()));
    EXPECT_TRUE(SortResultMatches(outiter.get(), sorted, n));

    extsort.reset();

    TDB_TEST_END
}

TEST_F(BasicTestExternalSort, TestSortUniformInt64Desc) {
    TDB_TEST_BEGIN

    const uint64_t seed = absl::GetFlag(FLAGS_test_sort_seed);
    const int64_t n = absl::GetFlag(FLAGS_test_nitems);
    const int64_t nways = absl::GetFlag(FLAGS_test_nways);
    const int64_t int64_lb = absl::GetFlag(FLAGS_test_int64_lb);
    const int64_t int64_ub = absl::GetFlag(FLAGS_test_int64_ub);
    PseudoRandomItemIterator<int64_t> iter(
        std::uniform_int_distribution<int64_t>(int64_lb, int64_ub),
        std::mt19937_64(seed), n);

    auto extsort = ExternalSort::Create(s_int_cmp_desc<int64_t>, nways);
    std::unique_ptr<ItemIterator> outiter;
    outiter = extsort->Sort(&iter);

    const int64_t *sorted;
    ASSERT_NO_ERROR(sorted = iter.GetSortedItems(std::greater<int64_t>()));
    EXPECT_TRUE(SortResultMatches(outiter.get(), sorted, n));

    extsort.reset();
    outiter.reset();

    TDB_TEST_END
}

TEST_F(BasicTestExternalSort, TestSortUniformStringAsc) {
    TDB_TEST_BEGIN

    const uint64_t seed = absl::GetFlag(FLAGS_test_sort_seed);
    const int64_t n = absl::GetFlag(FLAGS_test_nitems);
    const int64_t nways = absl::GetFlag(FLAGS_test_nways);
    const size_t strlen_lb = absl::GetFlag(FLAGS_test_strlen_lb);
    const size_t strlen_ub = absl::GetFlag(FLAGS_test_strlen_ub);
    PseudoRandomItemIterator<std::string> iter(
        RandomStringDistribution(strlen_lb, strlen_ub),
        std::mt19937_64(seed), n);

    auto extsort = ExternalSort::Create(s_str_cmp, nways);
    std::unique_ptr<ItemIterator> outiter;
    ASSERT_NO_ERROR(outiter = extsort->Sort(&iter));

    const std::string *sorted;
    ASSERT_NO_ERROR(sorted = iter.GetSortedItems(std::less<std::string>()));
    EXPECT_TRUE(SortResultMatches(outiter.get(), sorted, n));

    extsort.reset();
    outiter.reset();

    TDB_TEST_END
}

TEST_F(BasicTestExternalSort, TestSortUniformStringDesc) {
    TDB_TEST_BEGIN

    const uint64_t seed = absl::GetFlag(FLAGS_test_sort_seed);
    const int64_t n = absl::GetFlag(FLAGS_test_nitems);
    const int64_t nways = absl::GetFlag(FLAGS_test_nways);
    const size_t strlen_lb = absl::GetFlag(FLAGS_test_strlen_lb);
    const size_t strlen_ub = absl::GetFlag(FLAGS_test_strlen_ub);
    PseudoRandomItemIterator<std::string> iter(
        RandomStringDistribution(strlen_lb, strlen_ub),
        std::mt19937_64(seed), n);

    auto extsort = ExternalSort::Create(s_str_cmp_desc, nways);
    std::unique_ptr<ItemIterator> outiter;
    ASSERT_NO_ERROR(outiter = extsort->Sort(&iter));

    const std::string *sorted;
    ASSERT_NO_ERROR(sorted = iter.GetSortedItems(std::greater<std::string>()));
    EXPECT_TRUE(SortResultMatches(outiter.get(), sorted, n));

    extsort.reset();
    outiter.reset();

    TDB_TEST_END
}


}   // namespace taco

