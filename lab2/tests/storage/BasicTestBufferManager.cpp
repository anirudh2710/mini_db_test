#include "base/TDBDBTest.h"

#include <algorithm>

#include <absl/strings/str_format.h>

#include "storage/BufferManager.h"
#include "storage/FileManager.h"

// for getting the page number mapping to physical files
#include "storage/FileManager_private.h"

namespace taco {

static size_t s_npages_read = 0;
static size_t s_npages_written = 0;

// Note: never use ScopedBufferId in the BufferManager tests, as g_bufman
// does not point to our buffer manager!
class BasicTestBufferManager: public TDBDBTest {
protected:
    void SetUp() override {
        // Disabling the catalog cache prevents errors in its calls to the
        // buffer manager, which will prevent the entire test from running,
        // in case PersistentCatCache is used.
        g_test_no_catcache = true;

        // We'll be creating our own buffer manager in the tests. So disable
        // that in the global DB object.
        g_test_no_bufman = true;

        // parent class set up
        TDBDBTest::SetUp();

        // We'll populate the data file directly through FSFile instead of
        // FileManager, as it depends on the BufferManager to successfully
        // create a new virtual File. ReadPage or WritePage on the other hand,
        // does not rely on the BufferManager.
        std::string main_datapath = g_fileman->GetMainDataFilePath(0);

        // We need to close the database and reopen it so that the file manager
        // has the correct file size after we extend it.
        ASSERT_NO_ERROR(g_db->close());
        ASSERT_NO_ERROR(PopulateMainDataFile(main_datapath));
        const std::string &dbpath = g_db->GetLastDBPath();
        EXPECT_NO_ERROR(g_db->open(dbpath, GetBufferPoolSize(),
                                   /*create=*/false, /*overwrite=*/false));
        if (!g_db->is_open()) {
            throw TDBTestSetUpFailure("BasicTestBufferManager::SetUp() "
                "is unable to reopen the database");
        }

        m_bufman = new BufferManager();
        ASSERT(m_bufman);

        s_npages_read = 0;
        s_npages_written = 0;
    }

    void TearDown() override {
        TDBDBTest::TearDown();

        if (m_bufman)
            delete m_bufman;
    }

    void PopulateMainDataFile(const std::string &main_datapath) {
        std::unique_ptr<FSFile> fsfile = absl::WrapUnique(
            FSFile::Open(main_datapath, false, true, false));
        if (!fsfile.get()) {
            throw TDBTestSetUpFailure("Failed to open the main data file");
        }

        m_first_page_number = fsfile->Size() / PAGE_SIZE;
        fsfile->Allocate(m_num_file_pages * PAGE_SIZE);

        unique_malloced_ptr buf = unique_aligned_alloc(512, PAGE_SIZE);
        memset(buf.get(), 0, PAGE_SIZE);
        for (uint64_t n = 0; n < m_num_file_pages; ++n) {
            *(uint64_t*)(buf.get()) = MAGIC + (n ^ m_magic_mask);
            fsfile->Write(buf.get(), PAGE_SIZE,
                          (m_first_page_number + n)  * PAGE_SIZE);
        }

        fsfile->Close();
    }

    void RecreateBufman() {
        if (m_bufman)
            delete m_bufman;
        m_bufman = new BufferManager();
        ASSERT(m_bufman);
    }

    void CheckAllPages(
        absl::string_view callsite_file,
        uint64_t callsite_lineno,
        std::vector<BufferId> &bufids,
        std::function<void(BufferId)> post_pin_call =
            std::function<void(BufferId)>(),
        uint64_t expected_value_offset = 0) {

        char *buf;
        bufids.clear();
        // read the pages in a shuffled order
        for (uint64_t n = 0; n < m_num_file_pages; ++n) {
            PageNumber pid = m_first_page_number + (n ^ m_reorder_mask);
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
                MAGIC +(n ^ m_reorder_mask ^ m_magic_mask)
                + expected_value_offset;
            EXPECT_EQ(*(uint64_t*) buf, expected_value)
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

    BufferManager *m_bufman;
    PageNumber m_first_page_number;

public:
    static const PageNumber m_reorder_mask;
    static const PageNumber m_magic_mask;
    static const PageNumber m_num_file_pages;
    static const uint64_t MAGIC;
};

const PageNumber BasicTestBufferManager::m_reorder_mask = 0x2d;
const PageNumber BasicTestBufferManager::m_magic_mask = 0x31;
const PageNumber BasicTestBufferManager::m_num_file_pages = 64;
const uint64_t BasicTestBufferManager::MAGIC = 0xfaebdc0918274655ul;

// document the constraints of m_num_file_pages in the test here
static_assert(
    BasicTestBufferManager::m_num_file_pages >= 3 &&
    ((BasicTestBufferManager::m_num_file_pages & 1) == 0));

}   // namespace taco

extern "C" void
__wrap_taco_fileman_readpage_impl(::taco::FileManager *fileman,
                                  ::taco::PageNumber pid,
                                  char *buf) {
    __real_taco_fileman_readpage_impl(fileman, pid, buf);
    ++::taco::s_npages_read;
}

extern "C" void
__wrap_taco_fileman_writepage_impl(::taco::FileManager *fileman,
                                   ::taco::PageNumber pid,
                                   const char *buf) {
    __real_taco_fileman_writepage_impl(fileman, pid, buf);
    ++::taco::s_npages_written;
}

namespace taco {

// this allows these member functions to print where it is called when there's
// an error
#define CheckAllPages(...) CheckAllPages(__FILE__, __LINE__, __VA_ARGS__)
#define DropAllPins(...) DropAllPins(__FILE__, __LINE__, __VA_ARGS__)

TEST_F(BasicTestBufferManager, TestNormalInitAndDestroy) {
    TDB_TEST_BEGIN

    ASSERT_NO_ERROR(m_bufman->Init(1));
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, 0);
    EXPECT_EQ(s_npages_written, 0);
    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestPinPageWithoutEviction) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;

    // test one pin at a time
    ASSERT_NO_ERROR(m_bufman->Init(m_num_file_pages));
    CheckAllPages(bufids,
        [this](BufferId bufid) {ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));});
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);
    // these buffer ids should be all unique
    std::sort(bufids.begin(), bufids.end());
    auto bufids_unique_end = std::unique(bufids.begin(), bufids.end());
    uint64_t n_unique_bufids = bufids_unique_end - bufids.begin();
    EXPECT_EQ(n_unique_bufids, m_num_file_pages) << "expecting unique buffer ids";

    s_npages_read = 0;
    RecreateBufman();

    // test holding onto all pins
    ASSERT_NO_ERROR(m_bufman->Init(m_num_file_pages));
    CheckAllPages(bufids);
    DropAllPins(bufids);
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);
    // these buffer ids should be all unique
    std::sort(bufids.begin(), bufids.end());
    bufids_unique_end = std::unique(bufids.begin(), bufids.end());
    n_unique_bufids = bufids_unique_end - bufids.begin();
    EXPECT_EQ(n_unique_bufids, m_num_file_pages)
        << "expecting unique buffer ids";

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestPinPageWithEviction) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;

    // test one pin at a time with only 1 frame
    ASSERT_NO_ERROR(m_bufman->Init(1));
    CheckAllPages(bufids,
        [this](BufferId bufid) {ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));});
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    // these buffer ids should all the same
    EXPECT_TRUE(std::all_of(bufids.begin(), bufids.end(),
        [&](BufferId bufid) -> bool { return bufid == bufids[0]; }))
        << "expecting the same buffer ID for all pin request";

    s_npages_read = 0;
    RecreateBufman();

    // test one pin at a time with one fewer frame than needed
    ASSERT_NO_ERROR(m_bufman->Init(m_num_file_pages - 1));
    CheckAllPages(bufids,
        [this](BufferId bufid) {ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));});
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    // All but one buffer id should be unique
    std::sort(bufids.begin(), bufids.end());
    auto bufids_unique_end = std::unique(bufids.begin(), bufids.end());
    uint64_t n_unique_bufids = bufids_unique_end - bufids.begin();
    EXPECT_EQ(n_unique_bufids, m_num_file_pages - 1);

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestMultiplePins) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;
    char *buf;
    char *buf2;

    // test double pin with only one page pinned
    ASSERT_NO_ERROR(m_bufman->Init(2));
    BufferId bufid, bufid2;
    ASSERT_NO_ERROR(bufid = m_bufman->PinPage(m_first_page_number, &buf));
    ASSERT_NO_ERROR(bufid2 = m_bufman->PinPage(m_first_page_number, &buf2));
    EXPECT_EQ(bufid, bufid2);
    EXPECT_EQ(buf, buf2);
    uint64_t expected_value = MAGIC + m_magic_mask;
    EXPECT_EQ(*(uint64_t *) buf, expected_value);
    EXPECT_EQ(*(uint64_t *) buf2, expected_value);
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);

    // drop only one pin and we should still be able to go through all the pages
    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    CheckAllPages(bufids,
        [this](BufferId bufid){ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));});
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);
    EXPECT_EQ(bufids[m_reorder_mask], bufid)
        << "page " << m_first_page_number
        << " is pinned at a second buffer frame";
    bufids[m_reorder_mask] = INVALID_BUFID;
    // all other pages should be pinned at the second buffer frame
    EXPECT_TRUE(std::all_of(bufids.begin(), bufids.end(),
        [&](BufferId bufid) -> bool {
            return bufid == INVALID_BUFID ||
                bufid == bufids[1 ^ m_reorder_mask];
        }));

    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    s_npages_read = 0;
    RecreateBufman();

    // test double pin with many pages pinned
    ASSERT_NO_ERROR(m_bufman->Init(m_num_file_pages));
    CheckAllPages(bufids);
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    // pin the first page again and it should be found in the buffer pool
    bufid = bufids[m_reorder_mask];
    ASSERT_NO_ERROR(bufid2 = m_bufman->PinPage(m_first_page_number, &buf));
    ASSERT_EQ(bufid, bufid2);
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);
    expected_value = MAGIC + m_magic_mask;
    EXPECT_EQ(*(uint64_t*) buf, expected_value);

    // drop all the pins and we should still have one pin on it
    DropAllPins(bufids);
    // and thus we can drop the pin on page 0 again
    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestUnpinPageNotPinned) {
    TDB_TEST_BEGIN

    PageNumber valid_pid = m_first_page_number;
    BufferId bufid;
    char *buf;

    // Collect a valid buffer id that could be used to represent a buffer
    // frame, in case the buffer manager does not assign buffer ids from 0.
    // However, this assumes the buffer manager should always use the same
    // set of buffer ids given the same buffer pool size per spec.
    ASSERT_NO_ERROR(m_bufman->Init(2));
    ASSERT_NO_ERROR(bufid = m_bufman->PinPage(valid_pid, &buf));
    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);

    // unpin the page again should trigger an unpinned error. We also test
    // a few other functions that are also supposed to report unpinned errors.
    EXPECT_FATAL_ERROR(m_bufman->UnpinPage(bufid));
    EXPECT_FATAL_ERROR(m_bufman->MarkDirty(bufid));
    EXPECT_FATAL_ERROR(m_bufman->GetPageNumber(bufid));
    EXPECT_FATAL_ERROR(m_bufman->GetBuffer(bufid));
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);

    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);

    RecreateBufman();
    s_npages_read = 0;
    s_npages_written = 0;

    // With a new buffer manager, trying to unpin the previously found
    // valid buffer id should result in an unpinned error.
    ASSERT_NO_ERROR(m_bufman->Init(2));
    EXPECT_FATAL_ERROR(m_bufman->UnpinPage(bufid));
    EXPECT_FATAL_ERROR(m_bufman->MarkDirty(bufid));
    EXPECT_FATAL_ERROR(m_bufman->GetPageNumber(bufid));
    EXPECT_FATAL_ERROR(m_bufman->GetBuffer(bufid));
    EXPECT_EQ(s_npages_read, 0);
    EXPECT_EQ(s_npages_written, 0);

    // In any case the buffer pool must be in a valid state to finish the
    // destroy() call.
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, 0);
    EXPECT_EQ(s_npages_written, 0);

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestPinInvalidPid) {
    TDB_TEST_BEGIN

    PageNumber valid_pid = m_first_page_number,
               invalid_pid = m_first_page_number + m_num_file_pages,
               invalid_pid2 = DataFileIdAndPageIdGetPageNumber(1, 0);
    BufferId bufid, bufid2;
    uint64_t expected_value;
    char *buf;
    std::vector<BufferId> bufids;

    // keep a pin on a valid page
    ASSERT_NO_ERROR(m_bufman->Init(2));
    ASSERT_NO_ERROR(bufid = m_bufman->PinPage(valid_pid, &buf));
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);
    expected_value = MAGIC + m_magic_mask;
    EXPECT_EQ(*(uint64_t *) buf, expected_value);

    // pinning a non-existent page is fatal error (from the file manager)
    EXPECT_FATAL_ERROR(
        bufid2 = m_bufman->PinPage(invalid_pid, &buf),
        HasSubstr(absl::StrFormat(
            "trying to read a non-existent page " PAGENUMBER_FORMAT,
            invalid_pid)));
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);
    EXPECT_FATAL_ERROR(
        bufid2 = m_bufman->PinPage(invalid_pid2, &buf),
        HasSubstr(absl::StrFormat(
            "trying to read a non-existent page " PAGENUMBER_FORMAT,
            invalid_pid2)));
    EXPECT_EQ(s_npages_read, 1);
    EXPECT_EQ(s_npages_written, 0);

    // the buffer manager should still be in a valid state after the error!
    CheckAllPages(bufids,
        [this](BufferId bufid){ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));});
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    // The buffer pin on the first page should still be valid when
    // CheckAllPages() try to pin it and thus the buffer manager should return
    // the same buffer id for that. Any other pages should be pinned in the
    // other buffer frame.
    EXPECT_EQ(bufids[m_reorder_mask], bufid);
    bufids[m_reorder_mask] = INVALID_BUFID;
    EXPECT_TRUE(std::all_of(bufids.begin(), bufids.end(),
        [&](BufferId bufid) -> bool {
            return bufid == INVALID_BUFID ||
                   bufid == bufids[1 ^ m_reorder_mask];
        }));

    // Now unpin the first page and it should have a pin count of 0. It should
    // throw an error if we unpin it a second time.
    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    EXPECT_FATAL_ERROR(m_bufman->UnpinPage(bufid));

    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestNoAvailableBufferFrame) {
    TDB_TEST_BEGIN

    BufferId bufid, bufid2, bufid3;
    char *buf;

    ASSERT_NO_ERROR(m_bufman->Init(2));
    ASSERT_NO_ERROR(bufid = m_bufman->PinPage(m_first_page_number, &buf));
    ASSERT_NO_ERROR(bufid2 = m_bufman->PinPage(m_first_page_number + 1, &buf));
    EXPECT_NE(bufid, bufid2);
    EXPECT_EQ(s_npages_read, 2);
    EXPECT_EQ(s_npages_written, 0);

    // all buffer frames are pinned now. Next pin on a different page will
    // cause a "no evictable free frame" error.
    ASSERT_REGULAR_ERROR(
        bufid3 = m_bufman->PinPage(m_first_page_number + 2, &buf));
    EXPECT_EQ(s_npages_read, 2);
    EXPECT_EQ(s_npages_written, 0);

    // unpin a page and then we should be able to pin the third page
    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid2));
    ASSERT_NO_ERROR(
        bufid3 = m_bufman->PinPage(m_first_page_number + 2, &buf));
    EXPECT_EQ(s_npages_read, 3);
    EXPECT_EQ(s_npages_written, 0);
    EXPECT_EQ(bufid3, bufid2);
    uint64_t expected_value = MAGIC + (2 ^ m_magic_mask);
    EXPECT_EQ(*(uint64_t *) buf, expected_value);

    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid3));
    ASSERT_NO_ERROR(m_bufman->Destroy());

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestFlushOnDestroy) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;

    ASSERT_NO_ERROR(m_bufman->Init(m_num_file_pages));
    CheckAllPages(bufids,
        [this](BufferId bufid) {
        ASSERT_NO_ERROR(m_bufman->MarkDirty(bufid));
        ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
    });
    // no page eviction yet
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);
    ASSERT_NO_ERROR(m_bufman->Destroy());
    // this is when all the pages are flushed
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, m_num_file_pages);

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestEvictingDirtyPage) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;

    // tests evicting every time the only available frame has an evictable
    // dirty page
    ASSERT_NO_ERROR(m_bufman->Init(1));
    CheckAllPages(bufids,
        [this](BufferId bufid) {
            ASSERT_NO_ERROR(m_bufman->MarkDirty(bufid));
            ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
        });
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, m_num_file_pages - 1);
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, m_num_file_pages);

    RecreateBufman();
    s_npages_read = 0;
    s_npages_written = 0;

    // tests the dirty bit is correctly cleared after eviction by only
    // marking one of the page as dirty
    ASSERT_NO_ERROR(m_bufman->Init(1));
    CheckAllPages(bufids,
        [this](BufferId bufid) {
            PageNumber pid;
            ASSERT_NO_ERROR(pid = m_bufman->GetPageNumber(bufid));
            if (pid == m_first_page_number) {
                ASSERT_NO_ERROR(m_bufman->MarkDirty(bufid));
            }
            ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
        });
    ASSERT_NO_ERROR(m_bufman->Destroy());

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestPagePinnedInFlush) {
    TDB_TEST_BEGIN

    BufferId bufid;
    char *buf;

    ASSERT_NO_ERROR(m_bufman->Init(1));
    ASSERT_NO_ERROR(bufid = m_bufman->PinPage(m_first_page_number, &buf));
    EXPECT_FATAL_ERROR(m_bufman->Destroy());

    TDB_TEST_END
}

TEST_F(BasicTestBufferManager, TestPageModification) {
    TDB_TEST_BEGIN

    std::vector<BufferId> bufids;

    ASSERT_NO_ERROR(m_bufman->Init(m_num_file_pages / 2));
    CheckAllPages(bufids,
        [this](BufferId bufid) {
        char *buf;
        PageNumber pid;
        ASSERT_NO_ERROR(buf = m_bufman->GetBuffer(bufid));
        ASSERT_NO_ERROR(pid = m_bufman->GetPageNumber(bufid));
        *(uint64_t *) buf += m_num_file_pages;
        ASSERT_NO_ERROR(m_bufman->MarkDirty(bufid));
        if (pid & 1) {
            // only unpin the odd pages
            ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
        }
    });
    // no page eviction yet
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, m_num_file_pages / 2);
    // drop the remainder of the pins
    for (uint64_t n = 0; n < m_num_file_pages; ++n) {
        PageNumber pid = m_first_page_number + n;
        if (pid & 1)
            bufids[n ^ m_reorder_mask] = INVALID_BUFID;
    }
    DropAllPins(bufids);
    ASSERT_NO_ERROR(m_bufman->Destroy());
    // this is when all the pages are flushed
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, m_num_file_pages);

    // reload the database to see if the pages are really persisted
    delete m_bufman;
    ASSERT_NO_ERROR(g_db->close());
    const std::string &dbpath = g_db->GetLastDBPath();
    ASSERT_NO_ERROR(g_db->open(dbpath, GetBufferPoolSize(),
                               /*create=*/false, /*overwrite=*/false));
    ASSERT_TRUE(g_db->is_open());
    m_bufman = new BufferManager();
    ASSERT(m_bufman);
    s_npages_read = 0;
    s_npages_written = 0;

    // read all the pages again through the new buffer manager
    ASSERT_NO_ERROR(m_bufman->Init(1));
    CheckAllPages(bufids,
        [this](BufferId bufid) {
            ASSERT_NO_ERROR(m_bufman->UnpinPage(bufid));
        },
        /*expected_value_offset=*/m_num_file_pages);
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);
    ASSERT_NO_ERROR(m_bufman->Destroy());
    EXPECT_EQ(s_npages_read, m_num_file_pages);
    EXPECT_EQ(s_npages_written, 0);

    TDB_TEST_END
}

}   // namespace taco
