#include "base/TDBNonDBTest.h"

#include <tuple>
#include <type_traits>
#include <random>

#include <absl/flags/flag.h>
#include <absl/strings/string_view.h>
#include <absl/strings/str_cat.h>

#include "storage/VarlenDataPage.h"
#include "storage/FileManager.h"
#include "storage/Record.h"

ABSL_FLAG(uint32_t, test_varlen_dp_seed, 0x3f220108u,
          "The seed used for generating random record length in "
          "variable-length data page tests.");

namespace taco {

using RNG = std::mt19937;

static constexpr size_t PageHeaderSize = sizeof(PageHeaderData);
static_assert(std::is_trivially_destructible<VarlenDataPage>::value);

class BasicTestVarlenDataPage:
    public TDBNonDBTest,
    // params: (min_reclen, max_reclen), usr_data_sz
    public ::testing::WithParamInterface<std::tuple<std::pair<FieldOffset,
                                                              FieldOffset>,
                                                    FieldOffset>> {
protected:
    void SetUp() override {
        TDBNonDBTest::SetUp();
        // VarlenDataPage only needs to assume maxalign
        m_pagebuf = (char *) aligned_alloc(MAXALIGN_OF, PAGE_SIZE);
        m_rng.seed(absl::GetFlag(FLAGS_test_varlen_dp_seed));
        m_unif_reclen.param(
            std::uniform_int_distribution<FieldOffset>(
                GetMinRecLen(),
                GetMaxRecLen()).param());
    }

    void TearDown() override {
        free(m_pagebuf);
        TDBNonDBTest::TearDown();
    }

    void
    InitPageBuf(bool onefill_ph) {
        if (onefill_ph) {
            memset(m_pagebuf, 0xff, PageHeaderSize);
            memset(m_pagebuf + PageHeaderSize, 0, PAGE_SIZE - PageHeaderSize);
        } else {
            memset(m_pagebuf, 0, PAGE_SIZE);
        }
    }

    void
    CheckPh(absl::string_view callsite_file,
            uint64_t callsite_lineno,
            bool onefill_ph) {
        EXPECT_TRUE(std::all_of(m_pagebuf, m_pagebuf + PageHeaderSize,
                    onefill_ph ? (
                        [](unsigned char x) -> bool {
                            return x == (unsigned char) 0xff;
                        }
                    ) : (
                        [](unsigned char x) -> bool {
                            return x == 0;
                        }
                    )))
            << "page header was modified. Note called from " << callsite_file
            << ":" << callsite_lineno;
    }

    static FieldOffset
    GetMinRecLen() {
        return MAXALIGN(std::get<0>(GetParam()).first);
    }

    static FieldOffset
    GetMaxRecLen() {
        return MAXALIGN(std::get<0>(GetParam()).second);
    }

    FieldOffset
    GetRandomRecLen() {
        return m_unif_reclen(m_rng);
    }

    static FieldOffset
    GetUsrDataSz() {
        return std::get<1>(GetParam());
    }

    static bool
    MayFit() {
        constexpr static size_t min_fixed_overhead = MAXALIGN_OF;
        FieldOffset usr_data_sz = GetUsrDataSz();
        size_t min_sz_to_fit_one_tuple = PageHeaderSize +
                min_fixed_overhead
                + MAXALIGN(SHORTALIGN(usr_data_sz) + sizeof(FieldOffset))
                + MAXALIGN_OF;
        return min_sz_to_fit_one_tuple <= PAGE_SIZE;
    }

    static bool
    MayNotFit() {
        constexpr static size_t allowable_fixed_overhead = MAXALIGN(8);
        FieldOffset usr_data_sz = GetUsrDataSz();
        size_t allowable_sz_to_fit_one_tuple = PageHeaderSize
            + allowable_fixed_overhead + MAXALIGN(usr_data_sz)
            + 2 * MAXALIGN_OF;
        return allowable_sz_to_fit_one_tuple > PAGE_SIZE;
    }

    static FieldOffset
    GetAllowablePageUsage(FieldOffset cur_sz, FieldOffset reclen) {
        constexpr static size_t allowable_fixed_overhead = MAXALIGN(8);
        FieldOffset usr_data_sz = GetUsrDataSz();
        if (cur_sz == 0) {
            cur_sz = PageHeaderSize + allowable_fixed_overhead
                + MAXALIGN(usr_data_sz);
        }
        cur_sz += MAXALIGN(reclen) + 2 * sizeof(SlotId);
        return cur_sz;
    }

    char *m_pagebuf;
    RNG  m_rng;
    std::uniform_int_distribution<FieldOffset> m_unif_reclen;
};

#define DPTEST_BEGIN(n) \
    TDB_TEST_BEGIN \
    for (uint32_t _t = 0, _max_t = (n); _t < _max_t; ++_t) { \
        for (const bool onefill_ph : { false, true }) { \
            InitPageBuf(onefill_ph); {

#define DPTEST_END \
            } \
            CheckPh(__FILE__, __LINE__, onefill_ph); \
        } \
    } \
    TDB_TEST_END

#define CHECK_USR_DATA_BEGIN(dp) \
    do { \
        if (usr_data_sz > 0) { \
            char *usr_data = dp.GetUserData(); \
            ASSERT_TRUE(usr_data >= m_pagebuf && \
                usr_data <= m_pagebuf + PAGE_SIZE - MAXALIGN(usr_data_sz) && \
                (uintptr_t) usr_data == MAXALIGN((uintptr_t) usr_data)) \
                << (void*) usr_data << ' ' << (void*) m_pagebuf \
                << ' ' << (void*)(m_pagebuf - usr_data_sz); \
            memset(usr_data, 0x2f, usr_data_sz); \
        } \
    } while(0)

#define CHECK_USR_DATA_END(dp) \
    do { \
        if (usr_data_sz > 0) { \
            char *usr_data = dp.GetUserData(); \
            ASSERT_TRUE(usr_data >= m_pagebuf && \
                usr_data <= m_pagebuf + PAGE_SIZE - MAXALIGN(usr_data_sz) && \
                (uintptr_t) usr_data == MAXALIGN((uintptr_t) usr_data)) \
                << (void*) usr_data << ' ' << (void*) m_pagebuf \
                << ' ' << (void*)(m_pagebuf - usr_data_sz); \
            EXPECT_TRUE(std::all_of(usr_data, usr_data + usr_data_sz, \
                [](unsigned char c) -> bool { \
                    return c == (unsigned char) 0x2f; \
                })); \
        } \
    } while (0)


TEST_P(BasicTestVarlenDataPage, TestInitialize) {
    if (MayNotFit()) {
        if (MayFit()) {
            LOG(kInfo, "skipped");
            return ;
        }
        DPTEST_BEGIN(1)
        EXPECT_REGULAR_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                            GetUsrDataSz()));
        DPTEST_END
    } else {
        DPTEST_BEGIN(1)
        ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                       GetUsrDataSz()));
        for (int i = 0; i < 2; ++i) {
            VarlenDataPage dp(m_pagebuf);
            EXPECT_EQ(dp.GetMinSlotId(), MinSlotId);
            EXPECT_LT(dp.GetMaxSlotId(), dp.GetMinSlotId());
            EXPECT_EQ(dp.GetMinOccupiedSlotId(), INVALID_SID);
            EXPECT_EQ(dp.GetMaxOccupiedSlotId(), INVALID_SID);
            EXPECT_EQ(dp.GetRecordCount(), 0);

            if (GetUsrDataSz() > 0) {
                ptrdiff_t usr_data_off = -1;
                EXPECT_NO_ERROR(usr_data_off = dp.GetUserData() - m_pagebuf);
                EXPECT_GE(usr_data_off, 0);
                EXPECT_LE(usr_data_off,
                    (ptrdiff_t) PAGE_SIZE - MAXALIGN(GetUsrDataSz()));
                EXPECT_EQ(usr_data_off, MAXALIGN(usr_data_off));
            }
        }
        DPTEST_END
    }
}

TEST_P(BasicTestVarlenDataPage, TestInsertRecord) {
    if (MayNotFit()) {
        // skip
        return ;
    }

    DPTEST_BEGIN(3)
    FieldOffset usr_data_sz = GetUsrDataSz();
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   usr_data_sz));
    VarlenDataPage dp(m_pagebuf);
    CHECK_USR_DATA_BEGIN(dp);

    maxaligned_char_buf buf(GetMaxRecLen(), (char) 0xe7);
    Record rec(buf);
    constexpr PageNumber pid = 12345;
    rec.GetRecordId().pid = pid;

    // inserting records up to the capacity
    std::vector<SlotId> sids;
    std::vector<FieldOffset> reclens;
    sids.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    FieldOffset allowable_page_usage = 0;
    for (size_t i = 1; ; ++i) {
        buf[0] = (char) i;
        reclens.push_back(GetRandomRecLen());
        allowable_page_usage = GetAllowablePageUsage(allowable_page_usage,
                                                     reclens.back());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;

        if (allowable_page_usage > (FieldOffset) PAGE_SIZE) {
            ASSERT_NO_ERROR((void) dp.InsertRecord(rec));
            if (rec.GetRecordId().sid == INVALID_SID) {
                EXPECT_EQ(rec.GetRecordId().pid, pid);
                reclens.pop_back();
                break;
            }
        } else {
            ASSERT_TRUE(dp.InsertRecord(rec));
            ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
        }

        SlotId sid = rec.GetRecordId().sid;
        EXPECT_NE(sid, INVALID_SID);
        EXPECT_GE(sid, dp.GetMinOccupiedSlotId());
        EXPECT_LE(sid, dp.GetMaxOccupiedSlotId());
        EXPECT_EQ(rec.GetRecordId().pid, pid);
        EXPECT_EQ(dp.GetRecordCount(), i);
        EXPECT_TRUE(dp.IsOccupied(sid));
        EXPECT_TRUE(dp.IsOccupied(dp.GetMaxOccupiedSlotId()));
        sids.push_back(sid);
    }

    EXPECT_EQ((size_t) dp.GetRecordCount(), sids.size());
    if (sids.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }
    EXPECT_GE(dp.GetMaxOccupiedSlotId() - dp.GetMinOccupiedSlotId() + 1,
              sids.size());

    // check if the records are looking good
    VarlenDataPage dp2(m_pagebuf);
    EXPECT_EQ((size_t) dp2.GetRecordCount(), sids.size());
    for (size_t i = 1; i <= sids.size(); ++i) {
        SlotId sid = sids[i - 1];
        FieldOffset reclen = reclens[i - 1];
        EXPECT_TRUE(dp2.IsOccupied(sid));
        Record rec;
        ASSERT_NO_ERROR(rec = dp2.GetRecord(sid));
        EXPECT_EQ(MAXALIGN((uintptr_t) rec.GetData()), (uintptr_t) rec.GetData());
        EXPECT_EQ(MAXALIGN(rec.GetLength()), MAXALIGN(reclen));
        EXPECT_EQ(rec.GetData()[0], (char) i);
        EXPECT_TRUE(std::all_of(rec.GetData() + 1, rec.GetData() + reclen,
            [](char c) -> bool { return c == (char) 0xe7; }));
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestVarlenDataPage, TestEraseRecord) {
    if (MayNotFit()) {
        LOG(kInfo, "skipped");
        return ;
    }

    DPTEST_BEGIN(3)
    FieldOffset usr_data_sz = GetUsrDataSz();
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   usr_data_sz));
    VarlenDataPage dp(m_pagebuf);
    CHECK_USR_DATA_BEGIN(dp);

    maxaligned_char_buf buf(GetMaxRecLen(), (char) 0xd4);
    Record rec(buf);
    constexpr PageNumber pid = 12345;
    rec.GetRecordId().pid = pid;

    // inserting records up to the capacity
    std::vector<SlotId> sids;
    std::vector<FieldOffset> reclens;
    sids.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    for (size_t i = 1; ; ++i) {
        buf[0] = (char) i;
        reclens.push_back(GetRandomRecLen());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;
        if (dp.InsertRecord(rec), rec.GetRecordId().sid == INVALID_SID) {
            reclens.pop_back();
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    ASSERT_EQ(dp.GetRecordCount(), sids.size());
    if (sids.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // erase the one in the middle
    VarlenDataPage dp2(m_pagebuf);
    size_t idx_to_erase = sids.size() / 2;
    SlotId sid_to_erase = sids[idx_to_erase];
    EXPECT_TRUE(dp2.EraseRecord(sid_to_erase));
    EXPECT_EQ(dp2.GetRecordCount(), sids.size() - 1);

    if (sids.size() == 1) {
        // no record left, skip the rest of the test
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // erasing it again should return false
    if (sid_to_erase <= dp2.GetMaxSlotId()) {
        EXPECT_FALSE(dp2.EraseRecord(sid_to_erase));
        EXPECT_EQ(dp2.GetRecordCount(), sids.size() - 1);
    }

    // check if the records are looking good
    for (size_t i = 1; i <= sids.size(); ++i) {
        SlotId sid = sids[i - 1];
        if (i - 1 == idx_to_erase) {
            EXPECT_TRUE(sid_to_erase > dp2.GetMaxSlotId() ||
                        !dp2.IsOccupied(sid));
            continue;
        }
        FieldOffset reclen = reclens[i - 1];
        EXPECT_TRUE(dp2.IsOccupied(sid));
        char *buf, *buf2;
        FieldOffset reclen_;
        ASSERT_NO_ERROR(buf = dp2.GetRecordBuffer(sid, nullptr));
        ASSERT_NO_ERROR(buf2 = dp2.GetRecordBuffer(sid, &reclen_));
        EXPECT_EQ(buf, buf2);
        EXPECT_EQ(MAXALIGN(reclen_), MAXALIGN(reclen));
        EXPECT_EQ(MAXALIGN((uintptr_t) buf), (uintptr_t) buf);
        EXPECT_EQ(buf[0], (char) i);
        EXPECT_TRUE(std::all_of(buf + 1, buf + reclen,
            [](char c) -> bool { return c == (char) 0xd4; }));
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestVarlenDataPage, TestUpdateRecord) {
    if (MayNotFit() || GetMinRecLen() >= 820) {
        LOG(kInfo, "skipped");
        return ;
    }

    DPTEST_BEGIN(6)
    FieldOffset usr_data_sz = GetUsrDataSz();
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   usr_data_sz));
    VarlenDataPage dp(m_pagebuf);
    CHECK_USR_DATA_BEGIN(dp);

    maxaligned_char_buf buf(GetMaxRecLen(), (char) 0xd3);
    Record rec(buf);
    constexpr PageNumber pid = 12345;
    rec.GetRecordId().pid = pid;

    // need space for at least 4 records with additional space available
    FieldOffset allowable_page_usage = 0;
    std::vector<FieldOffset> reclens;
    reclens.reserve(4);
    size_t n = 0;
    for (; n < 5; ++n) {
        reclens.push_back(GetRandomRecLen());
        allowable_page_usage = GetAllowablePageUsage(allowable_page_usage,
                                                     reclens.back());
        if (allowable_page_usage > (FieldOffset) PAGE_SIZE) {
            // won't fit
            break;
        }
    }
    if (n < 5) {
        LOG(kInfo, "skipped, _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // inserting the first 3 records
    std::vector<SlotId> sids;
    sids.reserve(4);
    for (size_t i = 1; i <= n - 2; ++i) {
        buf[0] = (char) i;
        rec.GetLength() = reclens[i - 1];
        rec.GetRecordId().sid = INVALID_SID;
        ASSERT_TRUE(dp.InsertRecord(rec));
        ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
        sids.push_back(rec.GetRecordId().sid);
    }
    ASSERT_EQ(dp.GetRecordCount(), n - 2);

    // update the all records with the same length, which should happen
    // in place per specification.
    VarlenDataPage dp2(m_pagebuf);
    for (size_t i = 1; i <= n - 2; ++i) {
        SlotId sid = sids[i - 1];
        char *rbuf1, *rbuf2;
        ASSERT_NO_ERROR(rbuf1 = dp2.GetRecordBuffer(sid, nullptr));
        buf[0] = (char) (i + n);
        rec.GetLength() = reclens[i - 1];
        rec.GetRecordId().sid = MaxSlotId;
        ASSERT_TRUE(dp2.UpdateRecord(sid, rec));
        EXPECT_EQ(rec.GetRecordId().sid, sid);
        ASSERT_NO_ERROR(rbuf2 = dp2.GetRecordBuffer(sid, nullptr));
        EXPECT_EQ(rbuf1, rbuf2);
        EXPECT_EQ(rbuf1[0], (char) (i + n));
        EXPECT_TRUE(std::all_of(rbuf1 + 1, rbuf1 + reclens[i - 1],
            [](char c) -> bool { return c == (char) 0xd3; }));
    }

    // update the first or last record (by offset) on the page, with the 4th
    // length, which should fit without compaction
    std::vector<std::pair<uintptr_t, SlotId>> rbufs;
    rbufs.reserve(n);
    for (size_t i = 1; i <= n - 2; ++i) {
        char *rbuf;
        SlotId sid = sids[i - 1];
        ASSERT_NO_ERROR(rbuf = dp.GetRecordBuffer(sid, nullptr));
        rbufs.emplace_back(std::make_pair((uintptr_t) rbuf, sid));
    }
    std::sort(rbufs.begin(), rbufs.end());
    SlotId sid_to_update = rbufs[(_t & 1) ? (n - 3) : 0].second;
    buf[0] = (char) n - 1 + n;
    rec.GetLength() = reclens[n - 2];
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp2.UpdateRecord(sid_to_update, rec));
    EXPECT_EQ(rec.GetRecordId().sid, sid_to_update);

    // check if the records are looking good
    rbufs.clear();
    EXPECT_EQ(dp2.GetRecordCount(), n - 2);
    for (size_t i = 1; i <= n - 2; ++i) {
        SlotId sid = sids[i - 1];
        FieldOffset reclen;
        char expected_value;
        if (sid == sid_to_update) {
            expected_value = (char) n - 1 + n;
            reclen = reclens[n - 2];
        } else {
            expected_value = (char) (i + n);
            reclen = reclens[i - 1];
        }
        EXPECT_TRUE(dp2.IsOccupied(sid));
        Record rec_;
        ASSERT_NO_ERROR(rec_ = dp2.GetRecord(sid));
        EXPECT_EQ(MAXALIGN(rec_.GetLength()), MAXALIGN(reclen));
        EXPECT_EQ(rec_.GetData()[0], expected_value);
        EXPECT_TRUE(std::all_of(rec_.GetData() + 1, rec_.GetData() + reclen,
            [](char c) -> bool { return c == (char) 0xd3; }));
        rbufs.emplace_back(std::make_pair((uintptr_t) rec_.GetData(), sid));
    }

    // update the record in the middle (by offset) on the page, with the 5th
    // length, which should still fit without compaction
    std::sort(rbufs.begin(), rbufs.end());
    SlotId sid_to_update2 = rbufs[(n - 2) / 2].second;
    buf[0] = (char) n + n;
    rec.GetLength() = reclens[n - 1];
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp.UpdateRecord(sid_to_update2, rec));
    EXPECT_EQ(rec.GetRecordId().sid, sid_to_update2);

    // check if the records are looking good at this time
    EXPECT_EQ(dp2.GetRecordCount(), n - 2);
    for (size_t i = 1; i <= n - 2; ++i) {
        SlotId sid = sids[i - 1];
        FieldOffset reclen;
        char expected_value;
        // If sid_to_update and sid_to_update2 is the same, the latter update
        // will apply.
        if (sid == sid_to_update2) {
            expected_value = (char) n + n;
            reclen = reclens[n - 1];
        } else if (sid == sid_to_update) {
            expected_value = (char) n - 1 + n;
            reclen = reclens[n - 2];
        } else {
            expected_value = (char) (i + n);
            reclen = reclens[i - 1];
        }
        EXPECT_TRUE(dp2.IsOccupied(sid));
        Record rec_;
        ASSERT_NO_ERROR(rec_ = dp2.GetRecord(sid));
        EXPECT_EQ(MAXALIGN(rec_.GetLength()), MAXALIGN(reclen));
        EXPECT_EQ(rec_.GetData()[0], expected_value);
        EXPECT_TRUE(std::all_of(rec_.GetData() + 1, rec_.GetData() + reclen,
            [](char c) -> bool { return c == (char) 0xd3; }));
    }

    // now try to update a record that definitely won't fit on the page
    rec.GetData() = m_pagebuf;
    rec.GetLength() = PAGE_SIZE;
    rec.GetRecordId().sid = MaxSlotId;
    EXPECT_FALSE(dp2.UpdateRecord(sids[0], rec));

    // check once more if the records are looking good
    EXPECT_EQ(dp2.GetRecordCount(), n - 2);
    for (size_t i = 1; i <= n - 2; ++i) {
        SlotId sid = sids[i - 1];
        FieldOffset reclen;
        char expected_value;
        // If sid_to_update and sid_to_update2 is the same, the latter update
        // will apply.
        if (sid == sid_to_update2) {
            expected_value = (char) n + n;
            reclen = reclens[n - 1];
        } else if (sid == sid_to_update) {
            expected_value = (char) n - 1 + n;
            reclen = reclens[n - 2];
        } else {
            expected_value = (char) (i + n);
            reclen = reclens[i - 1];
        }
        EXPECT_TRUE(dp2.IsOccupied(sid));
        Record rec_;
        ASSERT_NO_ERROR(rec_ = dp2.GetRecord(sid));
        EXPECT_EQ(MAXALIGN(rec_.GetLength()), MAXALIGN(reclen));
        EXPECT_EQ(rec_.GetData()[0], expected_value);
        EXPECT_TRUE(std::all_of(rec_.GetData() + 1, rec_.GetData() + reclen,
            [](char c) -> bool { return c == (char) 0xd3; }));
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestVarlenDataPage, TestSidNotValidErrors) {
    FieldOffset allowable_page_usage = GetAllowablePageUsage(0, GetMinRecLen());
    if (allowable_page_usage > (FieldOffset) PAGE_SIZE) {
        LOG(kInfo, "skipped");
        return;
    }

    DPTEST_BEGIN(1)
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   GetUsrDataSz()));

    maxaligned_char_buf buf(GetMinRecLen(), (char) 0xe7);
    Record rec(buf);
    Record rec_too_long(m_pagebuf, PAGE_SIZE);

    for (int i = 0; i < 3; ++i) {
        VarlenDataPage dp(m_pagebuf);
        EXPECT_REGULAR_ERROR(dp.IsOccupied(INVALID_SID));
        EXPECT_REGULAR_ERROR(dp.IsOccupied(dp.GetMinSlotId() - 1));
        EXPECT_REGULAR_ERROR(dp.IsOccupied(dp.GetMaxSlotId() + 1));
        EXPECT_REGULAR_ERROR(dp.GetRecordBuffer(INVALID_SID, nullptr));
        EXPECT_REGULAR_ERROR(dp.GetRecordBuffer(dp.GetMinSlotId() - 1, nullptr));
        EXPECT_REGULAR_ERROR(dp.GetRecordBuffer(dp.GetMaxSlotId() + 1, nullptr));
        EXPECT_REGULAR_ERROR(dp.GetRecord(INVALID_SID));
        EXPECT_REGULAR_ERROR(dp.GetRecord(dp.GetMinSlotId() - 1));
        EXPECT_REGULAR_ERROR(dp.GetRecord(dp.GetMaxSlotId() + 1));
        EXPECT_REGULAR_ERROR(dp.EraseRecord(INVALID_SID));
        EXPECT_REGULAR_ERROR(dp.EraseRecord(dp.GetMinSlotId() - 1));
        EXPECT_REGULAR_ERROR(dp.EraseRecord(dp.GetMaxSlotId() + 1));
        EXPECT_REGULAR_ERROR(dp.UpdateRecord(INVALID_SID, rec));
        EXPECT_REGULAR_ERROR(dp.UpdateRecord(dp.GetMinSlotId() - 1, rec));
        EXPECT_REGULAR_ERROR(dp.UpdateRecord(dp.GetMaxSlotId() + 1, rec));

        if (i == 0) {
            EXPECT_EQ(dp.GetRecordCount(), 0);
            rec.GetRecordId().sid = INVALID_SID;
            EXPECT_TRUE(dp.InsertRecord(rec));
            EXPECT_NE(rec.GetRecordId().sid, INVALID_SID);
            EXPECT_EQ(dp.GetRecordCount(), 1);
            EXPECT_EQ(dp.GetMinOccupiedSlotId(), rec.GetRecordId().sid);
            EXPECT_EQ(dp.GetMaxOccupiedSlotId(), rec.GetRecordId().sid);
            Record rec_;
            ASSERT_NO_ERROR(rec_ = dp.GetRecord(rec.GetRecordId().sid));
            EXPECT_EQ(MAXALIGN((uintptr_t)rec_.GetData()), (uintptr_t) rec_.GetData());
            EXPECT_EQ(MAXALIGN(rec.GetLength()), MAXALIGN(rec_.GetLength()));
            EXPECT_EQ(0, memcmp(rec.GetData(), rec_.GetData(), rec.GetLength()));
        } else if (i == 1) {
            EXPECT_EQ(dp.GetRecordCount(), 1);
            rec_too_long.GetRecordId().sid = MaxSlotId;
            ASSERT_NO_ERROR(dp.InsertRecord(rec_too_long));
            EXPECT_EQ(rec_too_long.GetRecordId().sid, INVALID_SID);
            EXPECT_EQ(dp.GetRecordCount(), 1);
        }
    }

    DPTEST_END
}


TEST_P(BasicTestVarlenDataPage, TestInsertionWithCompactionExistingSlot) {
    if (MayNotFit() || GetMinRecLen() > 1024) {
        LOG(kInfo, "skipped");
        return ;
    }

    DPTEST_BEGIN(6)
    FieldOffset usr_data_sz = GetUsrDataSz();
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   usr_data_sz));
    VarlenDataPage dp(m_pagebuf);
    CHECK_USR_DATA_BEGIN(dp);

    maxaligned_char_buf buf(PAGE_SIZE, (char) 0xa3);
    Record rec(buf);
    constexpr PageNumber pid = 12345;
    rec.GetRecordId().pid = pid;

    // inserting records up to the capacity so that no 8-byte record (after
    // maxalign) may be inserted.
    std::vector<SlotId> sids;
    std::vector<FieldOffset> reclens;
    sids.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    bool fill_with_8bytes = false;
    for (size_t i = 1; ; ++i) {
        buf[0] = (char) i;
        reclens.push_back(fill_with_8bytes ? 1 : GetRandomRecLen());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;
        if (dp.InsertRecord(rec), rec.GetRecordId().sid == INVALID_SID) {
            if (reclens.back() > 8) {
                fill_with_8bytes = true;
                --i;
                reclens.pop_back();
                continue;
            }
            reclens.pop_back();
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    ASSERT_EQ(dp.GetRecordCount(), sids.size());
    // we need at least 4 records for the test
    if (sids.size() <= 4) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // Erase the one in the middle and one at one end. Then insert a single
    // record with their total length, thiw should work with compaction.
    size_t n = (size_t) sids.size();
    VarlenDataPage dp2(m_pagebuf);
    std::vector<std::pair<uintptr_t, SlotId>> rbufs;
    rbufs.reserve(n);
    for (size_t i = 1; i <= n; ++i) {
        char *rbuf;
        SlotId sid = sids[i - 1];
        ASSERT_NO_ERROR(rbuf = dp.GetRecordBuffer(sid, nullptr));
        rbufs.emplace_back(std::make_pair((uintptr_t) rbuf, sid));
    }
    std::sort(rbufs.begin(), rbufs.end());
    SlotId sid_to_erase1 = rbufs[n / 2].second;
    SlotId sid_to_erase2 = rbufs[(_t & 1) ? (n - 1) : 0].second;
    FieldOffset reclen1, reclen2;
    ASSERT_NO_ERROR(dp.GetRecordBuffer(sid_to_erase1, &reclen1));
    ASSERT_NO_ERROR(dp.GetRecordBuffer(sid_to_erase2, &reclen2));
    ASSERT_TRUE(dp.EraseRecord(sid_to_erase1));
    ASSERT_TRUE(dp.EraseRecord(sid_to_erase2));
    buf[0] = (char) n + 1;
    rec.GetLength() = reclen1 + reclen2; // it's ok to use maxaligned lengths
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp.InsertRecord(rec));
    EXPECT_EQ(rec.GetRecordId().pid, pid);
    ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
    SlotId new_sid = rec.GetRecordId().sid;
    if (new_sid != sid_to_erase1 && new_sid != sid_to_erase2)
        sids.push_back(new_sid);

    // check if the records are looking good
    EXPECT_EQ(dp.GetRecordCount(), n - 1);
    for (size_t i = 1; i <= sids.size(); ++i) {
        SlotId sid = sids[i - 1];
        char expected_value;
        FieldOffset reclen;
        if (sid == new_sid) {
            expected_value = (char) n + 1;
            reclen = reclen1 + reclen2;
        } else if (sid == sid_to_erase1 || sid == sid_to_erase2) {
            EXPECT_TRUE(sid > dp2.GetMaxSlotId() || !dp.IsOccupied(sid));
            continue;
        } else {
            expected_value = (char) i;
            reclen = reclens[i - 1];
        }
        EXPECT_TRUE(dp2.IsOccupied(sid));
        char *rbuf;
        FieldOffset reclen_;
        ASSERT_NO_ERROR(rbuf = dp2.GetRecordBuffer(sid, &reclen_));
        EXPECT_EQ(MAXALIGN(reclen_), MAXALIGN(reclen));
        EXPECT_EQ(MAXALIGN((uintptr_t) rbuf), (uintptr_t) rbuf);
        EXPECT_EQ(rbuf[0], expected_value);
        EXPECT_TRUE(std::all_of(rbuf + 1, rbuf + reclen,
            [](char c) -> bool { return c == (char) 0xa3; }));
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestVarlenDataPage, TestInsertionWithCompactionNewSlot) {
    if (MayNotFit() || GetMaxRecLen() <= 16 || GetMinRecLen() > 1024) {
        LOG(kInfo, "skipped");
        return ;
    }

    DPTEST_BEGIN(3)
    FieldOffset usr_data_sz = GetUsrDataSz();
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   usr_data_sz));
    VarlenDataPage dp(m_pagebuf);
    CHECK_USR_DATA_BEGIN(dp);

    maxaligned_char_buf buf(PAGE_SIZE, (char) 0xb4);
    Record rec(buf);
    constexpr PageNumber pid = 12345;
    rec.GetRecordId().pid = pid;

    // inserting records up to the capacity so that no 8-byte record (after
    // maxalign) may be inserted.
    std::vector<SlotId> sids;
    std::vector<FieldOffset> reclens;
    sids.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    bool fill_with_8bytes = false;
    for (size_t i = 1; ; ++i) {
        buf[0] = (char) i;
        reclens.push_back(fill_with_8bytes ? 1 : GetRandomRecLen());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;
        if (dp.InsertRecord(rec), rec.GetRecordId().sid == INVALID_SID) {
            if (reclens.back() > 8) {
                fill_with_8bytes = true;
                --i;
                reclens.pop_back();
                continue;
            }
            reclens.pop_back();
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    ASSERT_EQ(dp.GetRecordCount(), sids.size());
    // we need at least 4 records for the test
    if (sids.size() <= 4) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // Update any two nonconsecutive records (by offsets) that are longer than
    // 16 bytes with two records of 8 bytes, which should happen in place. Then
    // insert a record whose length is 24 less than total maxaligned length of
    // the old record, which is longer than either of the two gaps. The total
    // lengths of the three new records are 8 bytes shorter than before, which
    // leaves some room for the new slot. This should trigger a compaction with
    // a new slot.
    size_t n = (size_t) sids.size();
    VarlenDataPage dp2(m_pagebuf);
    std::vector<std::pair<uintptr_t, SlotId>> rbufs;
    rbufs.reserve(n);
    for (size_t i = 1; i <= n; ++i) {
        char *rbuf;
        SlotId sid = sids[i - 1];
        ASSERT_NO_ERROR(rbuf = dp2.GetRecordBuffer(sid, nullptr));
        rbufs.emplace_back(std::make_pair((uintptr_t) rbuf, sid));
    }
    std::sort(rbufs.begin(), rbufs.end());

    SlotId sid_to_update1 = INVALID_SID;
    for (size_t i = 1; i <= n; ++i) {
        SlotId sid = rbufs[i - 1].second;
        FieldOffset reclen;
        ASSERT_NO_ERROR(dp2.GetRecordBuffer(sid, &reclen));
        if (reclen > 16) {
            sid_to_update1 = sid;
            break;
        }
    }
    if (sid_to_update1 == INVALID_SID) {
        LOG(kInfo, "skipped (no > 16-byte record) _t = %u, onefill_ph = %d",
                   _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    SlotId sid_to_update2 = INVALID_SID;
    for (size_t i = n; i >= 1; --i) {
        SlotId sid = rbufs[i - 1].second;
        FieldOffset reclen;
        ASSERT_NO_ERROR(dp2.GetRecordBuffer(sid, &reclen));
        if (reclen > 16) {
            sid_to_update2 = sid;
            break;
        }
    }
    if (sid_to_update2 == sid_to_update1) {
        LOG(kInfo, "skipped (only one > 16-byte record) _t = %u, onefill_ph = %d",
                   _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    char *rbuf1, *rbuf2;
    FieldOffset reclen1, reclen2;
    ASSERT_NO_ERROR(rbuf1 = dp2.GetRecordBuffer(sid_to_update1, &reclen1));
    ASSERT_NO_ERROR(rbuf2 = dp2.GetRecordBuffer(sid_to_update2, &reclen2));
    if (rbuf1 + MAXALIGN(reclen1) == rbuf2) {
        LOG(kInfo, "skipped (consecutive > 16-byte records) _t = %u, onefill_ph = %d",
                   _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    reclen1 = MAXALIGN(reclen1);
    reclen2 = MAXALIGN(reclen2);
    ASSERT_GT(reclen1 + reclen2, 32);

    char *rbuf_;
    FieldOffset reclen_;
    buf[0] = (char) n + 1;
    rec.GetLength() = 8;
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp2.UpdateRecord(sid_to_update1, rec));
    ASSERT_EQ(rec.GetRecordId().sid, sid_to_update1);
    ASSERT_NO_ERROR(rbuf_ = dp2.GetRecordBuffer(sid_to_update1, &reclen_));
    EXPECT_EQ(MAXALIGN(reclen_), 8);
    ASSERT_EQ(rbuf_, rbuf1); // update should be in place
    buf[0] = (char) n + 2;
    rec.GetLength() = 8;
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp2.UpdateRecord(sid_to_update2, rec));
    ASSERT_EQ(rec.GetRecordId().sid, sid_to_update2);
    ASSERT_NO_ERROR(rbuf_ = dp2.GetRecordBuffer(sid_to_update2, &reclen_));
    EXPECT_EQ(MAXALIGN(reclen_), 8);
    // XXX disabling the following line because some students might be doing
    // eager compaction. As a result, rbuf2 may actually change.
    //ASSERT_EQ(rbuf_, rbuf2); // update should be in place

    FieldOffset new_reclen = reclen1 + reclen2 - 24;
    ASSERT_GT(new_reclen, reclen1 - 8);
    ASSERT_GT(new_reclen, reclen2 - 8);
    buf[0] = (char) n + 3;
    rec.GetLength() = new_reclen; // it's ok to use maxaligned lengths
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp.InsertRecord(rec));
    ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
    SlotId new_sid = rec.GetRecordId().sid;
    sids.push_back(new_sid);

    // check if the records are looking good
    EXPECT_EQ(dp.GetRecordCount(), n + 1);
    for (size_t i = 1; i <= sids.size(); ++i) {
        SlotId sid = sids[i - 1];
        char expected_value;
        FieldOffset reclen;
        if (sid == new_sid) {
            expected_value = (char) n + 3;
            reclen = new_reclen;
        } else if (sid == sid_to_update1) {
            expected_value = (char) n + 1;
            reclen = 8;
        } else if (sid == sid_to_update2) {
            expected_value = (char) n + 2;
            reclen = 8;
        } else {
            expected_value = (char) i;
            reclen = reclens[i - 1];
        }
        EXPECT_TRUE(dp2.IsOccupied(sid));
        char *rbuf;
        FieldOffset reclen_;
        ASSERT_NO_ERROR(rbuf = dp2.GetRecordBuffer(sid, &reclen_));
        EXPECT_EQ(MAXALIGN(reclen_), MAXALIGN(reclen));
        EXPECT_EQ(MAXALIGN((uintptr_t) rbuf), (uintptr_t) rbuf);
        EXPECT_EQ(rbuf[0], expected_value);
        EXPECT_TRUE(std::all_of(rbuf + 1, rbuf + reclen,
            [](char c) -> bool { return c == (char) 0xb4; }));
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestVarlenDataPage, TestUpdateWithCompaction) {
    if (MayNotFit() || GetMaxRecLen() <= 8 || GetMinRecLen() > 1024) {
        LOG(kInfo, "skipped");
        return ;
    }

    DPTEST_BEGIN(6)
    FieldOffset usr_data_sz = GetUsrDataSz();
    ASSERT_NO_ERROR(VarlenDataPage::InitializePage(m_pagebuf,
                                                   usr_data_sz));
    VarlenDataPage dp(m_pagebuf);
    CHECK_USR_DATA_BEGIN(dp);

    maxaligned_char_buf buf(PAGE_SIZE, (char) 0xfe);
    Record rec(buf);
    constexpr PageNumber pid = 12345;
    rec.GetRecordId().pid = pid;

    // inserting records up to the capacity so that no 8-byte record (after
    // maxalign) may be inserted.
    std::vector<SlotId> sids;
    std::vector<FieldOffset> reclens;
    sids.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    bool fill_with_8bytes = false;
    for (size_t i = 1; ; ++i) {
        buf[0] = (char) i;
        reclens.push_back(fill_with_8bytes ? 1 : GetRandomRecLen());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;
        if (dp.InsertRecord(rec), rec.GetRecordId().sid == INVALID_SID) {
            if (reclens.back() > 8) {
                fill_with_8bytes = true;
                --i;
                reclens.pop_back();
                continue;
            }
            reclens.pop_back();
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    ASSERT_EQ(dp.GetRecordCount(), sids.size());
    // we need at least 4 records for the test
    if (sids.size() <= 4) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // Update one in the middle that is longer than 8 bytes with an 8-byte
    // record, which should happen in place. Then update one not consecutive to
    // it, with the the total length of the old record in the middle and this
    // one less 8 bytes (or adding 8 bytes). This should trigger a compaction
    // that successfully update (or only erasing the record, resp.).
    size_t n = (size_t) sids.size();
    VarlenDataPage dp2(m_pagebuf);
    std::vector<std::pair<uintptr_t, SlotId>> rbufs;
    rbufs.reserve(n);
    for (size_t i = 1; i <= n; ++i) {
        char *rbuf;
        SlotId sid = sids[i - 1];
        ASSERT_NO_ERROR(rbuf = dp.GetRecordBuffer(sid, nullptr));
        rbufs.emplace_back(std::make_pair((uintptr_t) rbuf, sid));
    }
    std::sort(rbufs.begin(), rbufs.end());
    SlotId sid_to_update1 = INVALID_SID;
    for (size_t i = 2; i <= n - 1; ++i) {
        SlotId sid = rbufs[i - 1].second;
        FieldOffset reclen;
        ASSERT_NO_ERROR(dp2.GetRecordBuffer(sid, &reclen));
        if (reclen > 8) {
            sid_to_update1 = sid;
            break;
        }
    }
    if (sid_to_update1 == INVALID_SID) {
        LOG(kInfo, "skipped (no > 16-byte record) _t = %u, onefill_ph = %d",
                   _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }
    char *rbuf1;
    FieldOffset reclen1;
    ASSERT_NO_ERROR(rbuf1 = dp2.GetRecordBuffer(sid_to_update1, &reclen1));

    char *rbuf_;
    FieldOffset reclen_;
    buf[0] = (char) n + 1;
    rec.GetLength() = 8;
    rec.GetRecordId().sid = INVALID_SID;
    ASSERT_TRUE(dp2.UpdateRecord(sid_to_update1, rec));
    ASSERT_EQ(rec.GetRecordId().sid, sid_to_update1);
    ASSERT_NO_ERROR(rbuf_ = dp2.GetRecordBuffer(sid_to_update1, &reclen_));
    EXPECT_EQ(MAXALIGN(reclen_), 8);
    ASSERT_EQ(rbuf_, rbuf1); // update should be in place

    // the second one to update must not be consecutive to the first one
    SlotId sid_to_update2 = (rbufs[1].second == sid_to_update1) ?
        rbufs[n - 1].second : rbufs[0].second;
    reclen1 = MAXALIGN(reclen1);

    FieldOffset reclen2;
    ASSERT_NO_ERROR(dp2.GetRecordBuffer(sid_to_update2, &reclen2));
    reclen2 = MAXALIGN(reclen2);
    FieldOffset new_reclen;
    if (_t & 1) {
        // should fail
        new_reclen = reclen1 + reclen2 + 8;
    } else {
        // should succeed
        new_reclen = reclen1 + reclen2 - 8;
    }
    buf[0] = (char) n + 2;
    rec.GetLength() = new_reclen;
    rec.GetRecordId().sid = sid_to_update1; // impossible value after update call
    if (_t & 1) {
        ASSERT_TRUE(dp.UpdateRecord(sid_to_update2, rec));
        ASSERT_EQ(rec.GetRecordId().sid, INVALID_SID);
    } else {
        ASSERT_TRUE(dp.UpdateRecord(sid_to_update2, rec));
        ASSERT_EQ(rec.GetRecordId().sid, sid_to_update2);
    }

    // check if the records are looking good
    // rec count should be one less if the update call failed
    EXPECT_EQ(dp.GetRecordCount(), n - (_t & 1));
    for (size_t i = 1; i <= sids.size(); ++i) {
        SlotId sid = sids[i - 1];
        char expected_value;
        FieldOffset reclen;
        if (sid == sid_to_update1) {
            expected_value = (char) n + 1;
            reclen = 8;
        } else if (sid == sid_to_update2) {
            if (_t & 1) {
                EXPECT_TRUE(sid_to_update2 > dp2.GetMaxSlotId() ||
                            !dp2.IsOccupied(sid_to_update2));
                continue;
            }
            expected_value = (char) n + 2;
            reclen = new_reclen;
        } else {
            expected_value = (char) i;
            reclen = reclens[i - 1];
        }
        EXPECT_TRUE(dp2.IsOccupied(sid));
        char *rbuf;
        FieldOffset reclen_;
        ASSERT_NO_ERROR(rbuf = dp2.GetRecordBuffer(sid, &reclen_));
        EXPECT_EQ(MAXALIGN(reclen_), MAXALIGN(reclen));
        EXPECT_EQ(MAXALIGN((uintptr_t) rbuf), (uintptr_t) rbuf);
        EXPECT_EQ(rbuf[0], expected_value);
        EXPECT_TRUE(std::all_of(rbuf + 1, rbuf + reclen,
            [](char c) -> bool { return c == (char) 0xfe; }));
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

static const std::vector<std::pair<FieldOffset, FieldOffset>> s_test_reclens = {
    {8, 8},
    {16, 16},
    {24, 24},
    {32, 32},
    {48, 48},
    {80, 80},
    {204, 204},
    {800, 800},
    {4000, 4000},
    {PAGE_SIZE, PAGE_SIZE},
    {6, 16},
    {16, 32},
    {40, 80},
    {10, 100},
    {200, 400},
    {700, 800},
};

static const std::vector<FieldOffset> s_test_usr_data_sz = {
    0, 7, 64, 71, 4000, PAGE_SIZE
};

INSTANTIATE_TEST_SUITE_P(
    All,
    BasicTestVarlenDataPage,
    ::testing::Combine(
        ::testing::ValuesIn(s_test_reclens),
        ::testing::ValuesIn(s_test_usr_data_sz)),
    [](const ::testing::TestParamInfo<BasicTestVarlenDataPage::ParamType>& info) {
        return absl::StrCat(
            std::get<0>(info.param).first,
            "_",
            std::get<0>(info.param).second,
            "_",
            std::get<1>(info.param));
    }
);

}   // namespace taco

