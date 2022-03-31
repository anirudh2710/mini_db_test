#include "base/TDBNonDBTest.h"

#include <tuple>
#include <type_traits>
#include <random>

#include <absl/flags/flag.h>
#include <absl/strings/string_view.h>
#include <absl/strings/str_cat.h>
#include <absl/container/flat_hash_set.h>

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

class BasicTestSortedDataPage:
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
        ASSERT_TRUE(std::all_of(m_pagebuf, m_pagebuf + PageHeaderSize,
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
            ASSERT_TRUE(std::all_of(usr_data, usr_data + usr_data_sz, \
                [](unsigned char c) -> bool { \
                    return c == (unsigned char) 0x2f; \
                })); \
        } \
    } while (0)



TEST_P(BasicTestSortedDataPage, TestComputeFreeSpace) {
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
    FieldOffset reclens_sum = 0;
    sids.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    for (size_t i = 1; ; ++i) {
        buf[0] = (char) i;
        reclens.push_back(GetRandomRecLen());
        reclens_sum += reclens.back();

        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;

        // ComputeFreeSpace returns a optimistic estimate of the free space
        // If return -1 there's absolutely no space left,
        // otherwise there may or may not be space left
        if (dp.ComputeFreeSpace(usr_data_sz, i-1, reclens_sum) == -1) {
            ASSERT_NO_ERROR((void) dp.InsertRecord(rec));
            ASSERT_EQ(rec.GetRecordId().sid, INVALID_SID);
            break;
        } else {
            ASSERT_NO_ERROR((void)dp.InsertRecord(rec));
            if (rec.GetRecordId().sid == INVALID_SID) {
                break;
            }
        }
    }

    DPTEST_END
}

TEST_P(BasicTestSortedDataPage, TestInsertRecordDeterministic) {
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
    std::vector<char> expected_first_byte_of_recs;
    std::vector<FieldOffset> reclens;
    expected_first_byte_of_recs.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    FieldOffset allowable_page_usage = 0;
    for (size_t i = 1; ; ++i) {
        SlotId insert_loc = 1;

        buf[0] = (char) i;
        reclens.push_back(GetRandomRecLen());
        allowable_page_usage = GetAllowablePageUsage(allowable_page_usage,
                                                     reclens.back());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;

        if (allowable_page_usage > (FieldOffset) PAGE_SIZE) {
            ASSERT_NO_ERROR((void) dp.InsertRecordAt(insert_loc, rec));
            if (rec.GetRecordId().sid == INVALID_SID) {
                ASSERT_EQ(rec.GetRecordId().pid, pid);
                reclens.pop_back();
                break;
            }
        } else {
            ASSERT_TRUE(dp.InsertRecordAt(insert_loc, rec));
            ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
        }

        ASSERT_EQ(MAXALIGN((uintptr_t) rec.GetData()), (uintptr_t) rec.GetData());
        ASSERT_EQ(MAXALIGN(rec.GetLength()), MAXALIGN(reclens.back()));

        SlotId sid = rec.GetRecordId().sid;
        ASSERT_EQ(sid, insert_loc);
        ASSERT_GE(sid, dp.GetMinOccupiedSlotId());
        ASSERT_LE(sid, dp.GetMaxOccupiedSlotId());
        ASSERT_EQ(rec.GetRecordId().pid, pid);
        ASSERT_EQ(dp.GetRecordCount(), i);
        ASSERT_TRUE(dp.IsOccupied(sid));
        ASSERT_TRUE(dp.IsOccupied(dp.GetMaxOccupiedSlotId()));
        expected_first_byte_of_recs.push_back((char)i);
    }

    ASSERT_EQ((size_t) dp.GetRecordCount(), expected_first_byte_of_recs.size());
    if (expected_first_byte_of_recs.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }
    ASSERT_GE(dp.GetMaxOccupiedSlotId() - dp.GetMinOccupiedSlotId() + 1,
              expected_first_byte_of_recs.size());

    // check if the records are looking good
    std::vector<char> actual_first_byte_of_recs;
    VarlenDataPage dp2(m_pagebuf);
    ASSERT_EQ((size_t) dp2.GetRecordCount(), expected_first_byte_of_recs.size());
    for (size_t sid = dp2.GetMinSlotId(); sid <= dp2.GetMaxSlotId(); ++sid) {
        if (!dp2.IsOccupied(sid)) {
            continue;
        }
        Record rec;
        ASSERT_NO_ERROR(rec = dp2.GetRecord(sid));
        actual_first_byte_of_recs.emplace_back(rec.GetData()[0]);

        ASSERT_TRUE(std::all_of(rec.GetData() + 1, rec.GetData() + rec.GetLength(),
            [](char c) -> bool { return c == (char) 0xe7; }));
    }
    for (char first_byte: expected_first_byte_of_recs) {
        auto it = std::find(actual_first_byte_of_recs.begin(), actual_first_byte_of_recs.end(), first_byte);
        ASSERT_NE(it, actual_first_byte_of_recs.end());
        actual_first_byte_of_recs.erase(it);
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestSortedDataPage, TestInsertRecordRandom) {
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
    std::vector<char> expected_first_byte_of_recs;
    std::vector<FieldOffset> reclens;
    expected_first_byte_of_recs.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    FieldOffset allowable_page_usage = 0;
    for (size_t i = 1; ; ++i) {
        SlotId insert_loc = std::uniform_int_distribution<SlotId>(dp.GetMinSlotId(), dp.GetMaxSlotId()+1)(m_rng);

        buf[0] = (char) i;
        reclens.push_back(GetRandomRecLen());
        allowable_page_usage = GetAllowablePageUsage(allowable_page_usage,
                                                     reclens.back());
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;

        if (allowable_page_usage > (FieldOffset) PAGE_SIZE) {
            ASSERT_NO_ERROR((void) dp.InsertRecordAt(insert_loc, rec));
            if (rec.GetRecordId().sid == INVALID_SID) {
                ASSERT_EQ(rec.GetRecordId().pid, pid);
                reclens.pop_back();
                break;
            }
        } else {
            ASSERT_TRUE(dp.InsertRecordAt(insert_loc, rec));
            ASSERT_NE(rec.GetRecordId().sid, INVALID_SID);
        }

        ASSERT_EQ(MAXALIGN((uintptr_t) rec.GetData()), (uintptr_t) rec.GetData());
        ASSERT_EQ(MAXALIGN(rec.GetLength()), MAXALIGN(reclens.back()));

        SlotId sid = rec.GetRecordId().sid;
        ASSERT_EQ(sid, insert_loc);
        ASSERT_GE(sid, dp.GetMinOccupiedSlotId());
        ASSERT_LE(sid, dp.GetMaxOccupiedSlotId());
        ASSERT_EQ(rec.GetRecordId().pid, pid);
        ASSERT_EQ(dp.GetRecordCount(), i);
        ASSERT_TRUE(dp.IsOccupied(sid));
        ASSERT_TRUE(dp.IsOccupied(dp.GetMaxOccupiedSlotId()));
        expected_first_byte_of_recs.push_back((char)i);
    }

    ASSERT_EQ((size_t) dp.GetRecordCount(), expected_first_byte_of_recs.size());
    if (expected_first_byte_of_recs.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }
    ASSERT_GE(dp.GetMaxOccupiedSlotId() - dp.GetMinOccupiedSlotId() + 1,
              expected_first_byte_of_recs.size());

    // check if the records are looking good
    std::vector<char> actual_first_byte_of_recs;
    VarlenDataPage dp2(m_pagebuf);
    ASSERT_EQ((size_t) dp2.GetRecordCount(), expected_first_byte_of_recs.size());
    for (size_t sid = dp2.GetMinSlotId(); sid <= dp2.GetMaxSlotId(); ++sid) {
        if (!dp2.IsOccupied(sid)) {
            continue;
        }
        Record rec;
        ASSERT_NO_ERROR(rec = dp2.GetRecord(sid));
        actual_first_byte_of_recs.emplace_back(rec.GetData()[0]);

        ASSERT_TRUE(std::all_of(rec.GetData() + 1, rec.GetData() + rec.GetLength(),
            [](char c) -> bool { return c == (char) 0xe7; }));
    }
    for (char first_byte: expected_first_byte_of_recs) {
        auto it = std::find(actual_first_byte_of_recs.begin(), actual_first_byte_of_recs.end(), first_byte);
        ASSERT_NE(it, actual_first_byte_of_recs.end());
        actual_first_byte_of_recs.erase(it);
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestSortedDataPage, TestInsertRecordRandomOutOfBound) {
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
    std::vector<char> expected_first_byte_of_recs;
    std::vector<FieldOffset> reclens;
    expected_first_byte_of_recs.reserve(PAGE_SIZE / 8);
    reclens.reserve(PAGE_SIZE / 8);
    FieldOffset allowable_page_usage = 0;
    size_t num_inserted = 0;
    for (size_t i = 1; ; ++i) {
        // for 20% of chance, insert a record out of bound
        SlotId insert_loc = std::uniform_int_distribution<SlotId>(
            dp.GetMinSlotId(), (dp.GetMaxSlotId() + 1) * 1.2)(m_rng);

        buf[0] = (char) i;
        reclens.push_back(GetRandomRecLen());

        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;

        if (GetAllowablePageUsage(allowable_page_usage, reclens.back()) > (FieldOffset) PAGE_SIZE) {
            if (insert_loc > dp.GetMaxSlotId()+1) {
                ASSERT_REGULAR_ERROR((void) dp.InsertRecordAt(insert_loc, rec));
                continue;
            }
            ASSERT_NO_ERROR((void) dp.InsertRecordAt(insert_loc, rec));
            if (rec.GetRecordId().sid == INVALID_SID) {
                ASSERT_EQ(rec.GetRecordId().pid, pid);
                reclens.pop_back();
                break;
            }
        } else {
            if (insert_loc > dp.GetMaxSlotId()+1) {
                ASSERT_REGULAR_ERROR((void)dp.InsertRecordAt(insert_loc, rec));
                continue;
            }
            ASSERT_TRUE(dp.InsertRecordAt(insert_loc, rec));
            assert(rec.GetRecordId().sid != INVALID_SID);
        }

        allowable_page_usage = GetAllowablePageUsage(allowable_page_usage, reclens.back());
        num_inserted += 1;

        ASSERT_EQ(MAXALIGN((uintptr_t) rec.GetData()), (uintptr_t) rec.GetData());
        ASSERT_EQ(MAXALIGN(rec.GetLength()), MAXALIGN(reclens.back()));

        SlotId sid = rec.GetRecordId().sid;
        ASSERT_EQ(sid, insert_loc);
        ASSERT_GE(sid, dp.GetMinOccupiedSlotId());
        ASSERT_LE(sid, dp.GetMaxOccupiedSlotId());
        ASSERT_EQ(rec.GetRecordId().pid, pid);
        ASSERT_EQ(dp.GetRecordCount(), num_inserted);
        ASSERT_TRUE(dp.IsOccupied(sid));
        ASSERT_TRUE(dp.IsOccupied(dp.GetMaxOccupiedSlotId()));
        expected_first_byte_of_recs.push_back((char)i);
    }

    ASSERT_EQ((size_t) dp.GetRecordCount(), expected_first_byte_of_recs.size());
    if (expected_first_byte_of_recs.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }
    ASSERT_GE(dp.GetMaxOccupiedSlotId() - dp.GetMinOccupiedSlotId() + 1,
              expected_first_byte_of_recs.size());

    // check if the records are looking good
    std::vector<char> actual_first_byte_of_recs;
    VarlenDataPage dp2(m_pagebuf);
    ASSERT_EQ((size_t) dp2.GetRecordCount(), expected_first_byte_of_recs.size());
    for (size_t sid = dp2.GetMinSlotId(); sid <= dp2.GetMaxSlotId(); ++sid) {
        if (!dp2.IsOccupied(sid)) {
            continue;
        }
        Record rec;
        ASSERT_NO_ERROR(rec = dp2.GetRecord(sid));
        actual_first_byte_of_recs.emplace_back(rec.GetData()[0]);

        ASSERT_TRUE(std::all_of(rec.GetData() + 1, rec.GetData() + rec.GetLength(),
            [](char c) -> bool { return c == (char) 0xe7; }));
    }
    for (char first_byte: expected_first_byte_of_recs) {
        auto it = std::find(actual_first_byte_of_recs.begin(), actual_first_byte_of_recs.end(), first_byte);
        ASSERT_NE(it, actual_first_byte_of_recs.end());
        actual_first_byte_of_recs.erase(it);
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestSortedDataPage, TestRemoveAllRecordDeterministic) {
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

    VarlenDataPage dp2(m_pagebuf);
    for (size_t i = 1; i <= sids.size(); ++i) {
        ASSERT_TRUE(dp2.IsOccupied(1));
        ASSERT_NO_ERROR(dp2.RemoveSlot(1));
        ASSERT_EQ(dp2.GetRecordCount(), sids.size() - i);
    }

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestSortedDataPage, TestRemoveAllRecordRandomly) {
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

    VarlenDataPage dp2(m_pagebuf);

    size_t remaining_size = sids.size();
    while (dp2.GetRecordCount() > 0) {
        SlotId remove_slot_id = std::uniform_int_distribution<SlotId>(dp.GetMinSlotId(), dp.GetMaxSlotId())(m_rng);
        if (dp2.IsOccupied(remove_slot_id)) {
            remaining_size--;
        }
        ASSERT_NO_ERROR(dp2.RemoveSlot(remove_slot_id));
        ASSERT_EQ(dp2.GetRecordCount(), remaining_size);
    }
    ASSERT_EQ(remaining_size, 0);

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}

TEST_P(BasicTestSortedDataPage, TestRemoveAllRecordRandomlyOutOfBound) {
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

    VarlenDataPage dp2(m_pagebuf);

    size_t remaining_size = sids.size();
    while (dp2.GetRecordCount() > 0) {
        // for 20% of chance, remove a slot out of bound
        SlotId remove_slot_id = std::uniform_int_distribution<SlotId>(dp.GetMinSlotId(), dp.GetMaxSlotId()*1.2)(m_rng);
        if (remove_slot_id > dp.GetMaxSlotId()) {
            ASSERT_REGULAR_ERROR(dp2.RemoveSlot(remove_slot_id));
            continue;
        }
        if (dp2.IsOccupied(remove_slot_id)) {
            remaining_size--;
        }
        ASSERT_NO_ERROR(dp2.RemoveSlot(remove_slot_id));
        ASSERT_EQ(dp2.GetRecordCount(), remaining_size);
    }
    ASSERT_EQ(remaining_size, 0);

    CHECK_USR_DATA_END(dp2);
    DPTEST_END
}


TEST_P(BasicTestSortedDataPage, TestInsertionWithCompactionExistingSlot) {
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
    // record with their total length, this should work with compaction.
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
    char removed_first_byte;
    ASSERT_NO_ERROR(removed_first_byte = *dp.GetRecordBuffer(sid_to_erase1, &reclen1));
    ASSERT_NO_ERROR(removed_first_byte = *dp.GetRecordBuffer(sid_to_erase2, &reclen2));
    ASSERT_TRUE(dp.EraseRecord(sid_to_erase1));
    ASSERT_TRUE(dp.EraseRecord(sid_to_erase2));
    buf[0] = (char) n + 1;
    rec.GetLength() = reclen1 + reclen2; // it's ok to use maxaligned lengths
    rec.GetRecordId().sid = INVALID_SID;

    SlotId insert_slot_id;
    do {
        insert_slot_id = std::uniform_int_distribution<SlotId>(dp.GetMinSlotId(), dp.GetMaxSlotId())(m_rng);
    } while (insert_slot_id == sid_to_erase1 || insert_slot_id == sid_to_erase2);
    ASSERT_TRUE(dp.InsertRecordAt(insert_slot_id, rec));
    ASSERT_EQ(rec.GetRecordId().pid, pid);
    if (rec.GetRecordId().sid == INVALID_SID) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }
    SlotId new_sid = rec.GetRecordId().sid;
    if (new_sid != sid_to_erase1 && new_sid != sid_to_erase2)
        sids.push_back(new_sid);

    // check if the records are looking good
    std::vector<char> actual_first_byte;
    ASSERT_EQ(dp.GetRecordCount(), n - 1);
    for (SlotId sid: sids) {
        char expected_value;
        FieldOffset reclen;
        if (!dp.IsOccupied(sid)) {
            continue;
        } else if (sid == new_sid) {
            expected_value = (char) n + 1;
            reclen = reclen1 + reclen2;
        } else if (sid < new_sid) {
            expected_value = (char) sid;
            reclen = reclens[sid - 1];
        } else {
            expected_value = (char) sid - 1;
            reclen = reclens[sid - 2];
        }
        ASSERT_TRUE(dp2.IsOccupied(sid));
        char *rbuf;
        FieldOffset reclen_;
        ASSERT_NO_ERROR(rbuf = dp2.GetRecordBuffer(sid, &reclen_));
        ASSERT_EQ(MAXALIGN(reclen_), MAXALIGN(reclen));
        ASSERT_EQ(MAXALIGN((uintptr_t) rbuf), (uintptr_t) rbuf);
        ASSERT_EQ(rbuf[0], expected_value);
        ASSERT_TRUE(std::all_of(rbuf + 1, rbuf + reclen,
            [](char c) -> bool { return c == (char) 0xa3; }));
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
    BasicTestSortedDataPage,
    ::testing::Combine(
        ::testing::ValuesIn(s_test_reclens),
        ::testing::ValuesIn(s_test_usr_data_sz)),
    [](const ::testing::TestParamInfo<BasicTestSortedDataPage::ParamType>& info) {
        return absl::StrCat(
            std::get<0>(info.param).first,
            "_",
            std::get<0>(info.param).second,
            "_",
            std::get<1>(info.param));
    }
);

}   // namespace taco

