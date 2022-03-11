#include "base/TDBDBTest.h"

#include <random>
#include <absl/flags/flag.h>

#include "storage/BufferManager.h"
#include "storage/FileManager.h"
#include "utils/misc.h"
#include "utils/TruncatedZipfian.h"

#ifndef USE_JEMALLOC
#warning "BasicTestEvictionPolicy may not work correctly without jemalloc"
#endif

ABSL_FLAG(uint32_t, test_eviction_seed, 0xef220108u,
        "The seed used in the random number generator of eviction test.");
ABSL_FLAG(uint32_t, reorder_mask, 0xde374,
        "The xor mask used for reordering page access.");

namespace taco {

using RNG = std::mt19937;

class BasicTestEvictionPolicy;

// a flag to inform the read/write wrapper to switch the fake in-memory storage
static bool s_test_setup_finished = false;
static BasicTestEvictionPolicy *s_cur_test = nullptr;
static size_t s_npages_read = 0;
static size_t s_npages_written = 0;

class BasicTestEvictionPolicy: public TDBDBTest {
protected:
    void SetUp() override {
        m_reorder_mask = absl::GetFlag(FLAGS_reorder_mask) & m_page_seqno_mask;

        // this is mostly the same as BasicTestBufferManager except
        // that we fake the read/write interface with an in-memory storage
        s_test_setup_finished = false;
        g_test_no_catcache = true;
        g_test_no_bufman = true;
        TDBDBTest::SetUp();

        // initialize the in-memory pages
        m_page_values.reserve(m_num_file_pages);
        for (uint64_t i = 0; i < m_num_file_pages; ++i) {
            m_page_values[i] = MAGIC + (i ^ m_magic_mask);
        }
        s_cur_test = this;
        s_test_setup_finished = true;
        s_npages_read = 0;
        s_npages_written = 0;

        m_bufman = nullptr;
    }

    void TearDown() override {
        TDBDBTest::TearDown();

        if (m_bufman) {
            delete m_bufman;
            m_bufman = nullptr;
        }
    }

    static uint64_t
    GetMaxAllowableMem(BufferId buffer_size) {
        return m_allowable_mem_overhead_per_bufman +
            (PAGE_SIZE + m_allowable_mem_overhead_per_frame) * buffer_size;
    }

    void CheckAllPages(
        absl::string_view callsite_file,
        uint64_t callsite_lineno,
        std::vector<BufferId> &bufids,
        std::function<void(BufferId)> post_pin_call =
            std::function<void(BufferId)>(),
        uint64_t expected_value_offset = 0) {
        CheckAllPages(callsite_file, callsite_lineno, bufids,
            m_num_file_pages,
            [](uint64_t i) -> PageNumber {
                return 1 + (i ^ m_reorder_mask);
            },
            post_pin_call,
            expected_value_offset);
    }

    void CheckAllPages(
        absl::string_view callsite_file,
        uint64_t callsite_lineno,
        std::vector<BufferId> &bufids,
        uint64_t npages_to_check,
        std::function<PageNumber(uint64_t)> get_ith_page,
        std::function<void(BufferId)> post_pin_call =
            std::function<void(BufferId)>(),
        uint64_t expected_value_offset = 0) {

        char *buf;
        bufids.clear();
        // read the pages in a shuffled order
        for (uint64_t i = 0; i < npages_to_check; ++i) {
            PageNumber pid = get_ith_page(i);
            BufferId bufid;
            ASSERT_NO_ERROR(bufid = m_bufman->PinPage(pid, &buf))
                << "Note: called from " << callsite_file << ":"
                << callsite_lineno;
            bufids.push_back((BufferId) bufid);
            ASSERT_NE((BufferId) bufid, INVALID_BUFID)
                << "Note: called from " << callsite_file << ":"
                << callsite_lineno;
            EXPECT_EQ(m_bufman->GetPageNumber(bufid), pid)
                << "Note: called from " << callsite_file << ":"
                << callsite_lineno;
            EXPECT_EQ(m_bufman->GetBuffer(bufid), buf)
                << "Note: called from " << callsite_file << ":"
                << callsite_lineno;
            uint64_t expected_value =
                MAGIC + ((pid - 1) ^ m_magic_mask)
                + expected_value_offset;
            // Using assert_eq instead of expect_eq here so that we don't end
            // up with too many error messages if the buffer manager read a
            // wrong page.
            ASSERT_EQ(*(uint64_t*) buf, expected_value)
                << "for page " << pid << std::endl
                << "Note: called from " << callsite_file << ":"
                << callsite_lineno;
            if ((bool) post_pin_call)
                post_pin_call((BufferId) bufid);
        }
    }

    void DropAllPins(absl::string_view callsite_file,
                     uint64_t callsite_lineno,
                     const std::vector<BufferId> &bufids) {
        for (BufferId bufid: bufids) {
            // some test may mark some of the bufid as invalid to indicate they
            // have been or will be unpinned by the test itself.
            if (bufid != INVALID_BUFID) {
                ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid))
                    << "Note: called from " << callsite_file << ":"
                    << callsite_lineno;
            }
        }
    }

    BufferManager           *m_bufman;

    //! The first uint64_t values on the in-memory pages.
    std::vector<uint64_t>   m_page_values;

public:
    static const PageNumber m_num_file_pages;
    static const PageNumber m_page_seqno_mask;
    static PageNumber m_reorder_mask;
    static const PageNumber m_magic_mask;
    static const uint64_t MAGIC;

    static const uint64_t m_allowable_mem_overhead_per_frame;
    static const uint64_t m_allowable_mem_overhead_per_bufman;

    static const uint64_t m_small_bufsz;
    static const uint64_t m_medium_bufsz;
    static const uint64_t m_large_bufsz;

    friend void ::__wrap_taco_fileman_readpage_impl(::taco::FileManager *fileman,
                                                    ::taco::PageNumber pid,
                                                    char *buf);
    friend void ::__wrap_taco_fileman_writepage_impl(::taco::FileManager *fileman,
                                                     ::taco::PageNumber pid,
                                                     const char *buf);
};

// 1024 * 1024
const PageNumber BasicTestEvictionPolicy::m_num_file_pages = 0x100000;
const PageNumber BasicTestEvictionPolicy::m_page_seqno_mask = 0xfffff;
const PageNumber BasicTestEvictionPolicy::m_magic_mask = 0x3e02f;
PageNumber BasicTestEvictionPolicy::m_reorder_mask;
const uint64_t BasicTestEvictionPolicy::MAGIC = 0xfaebdc0918274655ul;
const uint64_t BasicTestEvictionPolicy::m_allowable_mem_overhead_per_frame = 96;
const uint64_t BasicTestEvictionPolicy::m_allowable_mem_overhead_per_bufman = 20000;
const uint64_t BasicTestEvictionPolicy::m_small_bufsz = 1024 * 16;
const uint64_t BasicTestEvictionPolicy::m_medium_bufsz = 1024 * 256;
const uint64_t BasicTestEvictionPolicy::m_large_bufsz = 1024 * 512;

// no overflow in all the magic numbers
static_assert(BasicTestEvictionPolicy::MAGIC +
    (BasicTestEvictionPolicy::m_num_file_pages - 1) >
    // because of the zipfian distribution used in TestEvictionWithHotSpot
    BasicTestEvictionPolicy::MAGIC &&
    BasicTestEvictionPolicy::m_num_file_pages >= 1024 * 1024);

}   // namespace taco

extern "C" void
__wrap_taco_fileman_readpage_impl(::taco::FileManager *fileman,
                                  ::taco::PageNumber pid,
                                  char *buf) {
    if (!taco::s_test_setup_finished) {
        __real_taco_fileman_readpage_impl(fileman, pid, buf);
        return ;
    }
    *(uint64_t*) buf = taco::s_cur_test->m_page_values[pid - 1];
    ++taco::s_npages_read;
}

extern "C" void
__wrap_taco_fileman_writepage_impl(::taco::FileManager *fileman,
                                   ::taco::PageNumber pid,
                                   const char *buf) {
    if (!taco::s_test_setup_finished) {
        __real_taco_fileman_writepage_impl(fileman, pid, buf);
        return ;
    }
    // no write needed as none of the following tests actually modify the pages
    //taco::s_cur_test->m_page_values[pid - 1] = *(const uint64_t*) buf;
    ++taco::s_npages_written;
}

namespace taco {

#define CheckAllPages(...) CheckAllPages(__FILE__, __LINE__, __VA_ARGS__)
#define DropAllPins(...) DropAllPins(__FILE__, __LINE__, __VA_ARGS__)

/*!
 * TestMemoryLimit is actually testing whether the buffer manager has a bounded
 * memory usage given a buffer size rather than testing the eviction policy.
 * However, we put it here in BasicTestEvictionPolicy so that we test larger
 * buffer sizes with the wrapped in-memory I/O rather than true disk I/Os.
 */
TEST_F(BasicTestEvictionPolicy, TestMemoryLimit) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;
    bufids.reserve(m_num_file_pages);

    for (BufferId buffer_size: {m_small_bufsz, m_medium_bufsz, m_large_bufsz,
                                (BufferId) m_num_file_pages}) {
        s_npages_read = 0;
        s_npages_written = 0;
        LOG(kInfo, "testing buffer size = %lu", (size_t) buffer_size);
        size_t datasz_begin = GetCurrentDataSize();
        m_bufman = new BufferManager();
        ASSERT_NO_ERROR(m_bufman->Init(buffer_size));
        size_t datasz_after_init = GetCurrentDataSize();
        EXPECT_LE(datasz_after_init - datasz_begin,
                  GetMaxAllowableMem(buffer_size));
        // reading all the pages once, the memusage should still within the
        // allowable limits
        CheckAllPages(bufids,
            [this](BufferId bufid) {
                ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
            });
        size_t datasz_after_checkallpages = GetCurrentDataSize();
        EXPECT_LE(datasz_after_checkallpages - datasz_begin,
                  GetMaxAllowableMem(buffer_size));
        EXPECT_EQ(s_npages_read, m_num_file_pages);
        EXPECT_EQ(s_npages_written, 0);

        ASSERT_NO_ERROR(m_bufman->Destroy());
        delete m_bufman;
        m_bufman = nullptr;
    }

    TDB_TEST_END
}

TEST_F(BasicTestEvictionPolicy, TestEvictionRateWithHotSpot) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;
    const double zipfian_s = 1.6;
    const double zipfian_ptrunc = 0.03;
    const BufferId xs_bufsz = 6;
    const uint32_t seed = absl::GetFlag(FLAGS_test_eviction_seed);
    RNG rng(seed);

    // Note: zipf generates numbers from 0 to N - 1 while the page numbers
    // starts from 1.
    TruncatedZipfian zipf(m_num_file_pages, zipfian_s, zipfian_ptrunc);
    const uint32_t nnottrunc = zipf.GetMinTruncatedItem();
    const uint32_t ntrunc = m_num_file_pages - nnottrunc;
    LOG(kInfo, "nnottrunc = %u, ntrunc = %u", nnottrunc, ntrunc);
    ASSERT_LT(nnottrunc, xs_bufsz)
        << "expecting a more skewed zipf distribution";

    const double trunc_prob = zipf.GetTotalTruncatedProbability();
    const double last_nontrunc_prob =
        zipf.GetProbability(zipf.GetMinTruncatedItem() - 1);
    LOG(kInfo, "last_nontrunc_prob = %f, trunc_prob = %f", last_nontrunc_prob,
        trunc_prob);
    const uint32_t nsamples = 1000000;
    LOG(kInfo, "nsamples = %u" ,nsamples);

    double eprob_nottrunc = 0.0;
    for (uint32_t i = 0; i < nnottrunc; ++i) {
        double px = zipf.GetProbability(i);
        double eviction_prob_ub = exp(xs_bufsz * log(1 - px));
        eprob_nottrunc += eviction_prob_ub * px / (1 - trunc_prob);
    }
    LOG(kInfo, "eprob_nottrunc = %f", eprob_nottrunc);
    uint64_t expected_eviction_ub =
        (uint64_t) std::ceil(nsamples * trunc_prob + nsamples * (1 - trunc_prob) * eprob_nottrunc);
    LOG(kInfo, "expected_eviction_ub = %lu (%f%%)", expected_eviction_ub, 1.0 * expected_eviction_ub / nsamples);

    // test for three times
    for (BufferId buffer_size: { xs_bufsz, xs_bufsz, xs_bufsz }) {
        s_npages_read = 0;
        s_npages_written = 0;
        LOG(kInfo, "testing buffer size = %lu", (size_t) buffer_size);
        m_bufman = new BufferManager();
        ASSERT_NO_ERROR(m_bufman->Init(buffer_size));

        CheckAllPages(bufids, /*npages_to_check=*/nsamples,
            /*get_ith_page=*/[&](uint64_t i) -> PageNumber {
                uint32_t x = zipf(rng);
                return (PageNumber)((x ^ m_reorder_mask) + 1);
            },
            /*post_pin_call=*/[this](BufferId bufid) {
                // mark it dirty so that any evicted page will be counted
                // by our write wrapper
                m_bufman->MarkDirty(bufid);
                m_bufman->UnpinPage(bufid);
            });
        LOG(kInfo, "s_npages_written = %u", s_npages_written);
        EXPECT_LE(s_npages_written, expected_eviction_ub)
            << "had more evictions than expected";
        LOG(kInfo, "diff = %ld", (int64_t) expected_eviction_ub - (int64_t) s_npages_written);

        ASSERT_NO_ERROR(m_bufman->Destroy());
        delete m_bufman;
        m_bufman = nullptr;
    }

    TDB_TEST_END
}

}   // namespace taco


