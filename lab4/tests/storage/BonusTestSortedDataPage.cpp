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

class BonusTestSortedDataPage:
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

TEST_P(BonusTestSortedDataPage, TestShiftSlotsToRight) {
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

        ASSERT_NO_ERROR((void)dp.InsertRecord(rec));
        if (rec.GetRecordId().sid == INVALID_SID) {
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    if (sids.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    SlotId origin_record_count = dp.GetRecordCount();
    SlotId origin_max_occupied_sid = dp.GetMaxOccupiedSlotId();

    // for 20% of chance, shift more slots than there're in the page
    FieldOffset n = std::uniform_int_distribution<SlotId>(dp.GetMinSlotId(), dp.GetMaxSlotId()*1.2)(m_rng);

    ASSERT_NO_ERROR(dp.ShiftSlots(n, true));
    SlotId remaining_record_count = n >= origin_record_count ? 0 : origin_record_count - n;
    ASSERT_EQ(dp.GetRecordCount(), remaining_record_count);
    if (remaining_record_count != 0)
        ASSERT_EQ(dp.GetMaxOccupiedSlotId(), origin_max_occupied_sid - n);
    else
        ASSERT_EQ(dp.GetMaxOccupiedSlotId(), 0);
    DPTEST_END
}

TEST_P(BonusTestSortedDataPage, TestShiftSlotsToRightWithInvalidSid) {
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

        ASSERT_NO_ERROR((void)dp.InsertRecord(rec));
        if (rec.GetRecordId().sid == INVALID_SID) {
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    if (sids.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // erase some records
    for (SlotId sid: sids) {
        if (sid % 2 == 1) {
            ASSERT_TRUE(dp.EraseRecord(sid));
        }
    }
    if (dp.GetRecordCount() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    SlotId origin_record_count = dp.GetRecordCount();
    SlotId origin_max_occupied_sid = dp.GetMaxOccupiedSlotId();

    FieldOffset n = std::uniform_int_distribution<SlotId>(dp.GetMinSlotId(), dp.GetMaxSlotId())(m_rng);

    ASSERT_NO_ERROR(dp.ShiftSlots(n, true));
    SlotId remaining_record_count = origin_record_count - (n / 2);
    ASSERT_EQ(dp.GetRecordCount(), remaining_record_count);
    ASSERT_EQ(dp.GetMaxOccupiedSlotId(), origin_max_occupied_sid - n);

    DPTEST_END
}

TEST_P(BonusTestSortedDataPage, TestShiftSlotsToRightThenToLeft) {
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

        ASSERT_NO_ERROR((void)dp.InsertRecord(rec));
        if (rec.GetRecordId().sid == INVALID_SID) {
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    if (sids.size() <= 1) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    SlotId origin_record_count = dp.GetRecordCount();
    SlotId origin_max_occupied_sid = dp.GetMaxOccupiedSlotId();

    // We intentionally avoid the case where all the slots are truncated and
    // then we reserve them as invalid slots, as this will never happen in its
    // current use case of b-tree page rebalancing.
    //
    // However, if you wonder what should happen there, it should still reserve
    // n unoccupied slots despite all the slots being unoccupied after the
    // second ShiftSlots call. This will temporarily violate the contract where
    // the left-most slot must always be occupied, but the rationale is that
    // the caller will immediately fill all these reserved slots using
    // InsertRecordAt() before doing anything else (assuming that the
    // GetMaxSlot() implementation is correct and does not check for whether
    // the left-most slot is occupied or now).
    SlotId n = std::uniform_int_distribution<SlotId>(
        dp.GetMinSlotId(), dp.GetMaxSlotId() - 1)(m_rng);

    ASSERT_NO_ERROR(dp.ShiftSlots(n, true));
    ASSERT_NO_ERROR(dp.ShiftSlots(n, false));
    ASSERT_EQ(dp.GetRecordCount(), origin_record_count - n);
    ASSERT_EQ(dp.GetMaxOccupiedSlotId(), origin_max_occupied_sid);

    for (SlotId sid = dp.GetMinSlotId(); sid <= dp.GetMaxSlotId(); ++sid) {
        if (sid <= n) {
            ASSERT_FALSE(dp.IsOccupied(sid));
        } else {
            ASSERT_TRUE(dp.IsOccupied(sid));
        }
    }
    DPTEST_END
}

TEST_P(BonusTestSortedDataPage, TestShiftSlotsToLeftNoSpace) {
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

        ASSERT_NO_ERROR((void)dp.InsertRecord(rec));
        if (rec.GetRecordId().sid == INVALID_SID) {
            break;
        }
        sids.push_back(rec.GetRecordId().sid);
    }
    if (sids.size() == 0) {
        LOG(kInfo, "skipped _t = %u, onefill_ph = %d", _t, (int) onefill_ph);
        CHECK_USR_DATA_END(dp);
        continue;
    }

    // fill up all spaces with empty records, so that slots can't be shifted left
    while (true) {
        reclens.push_back(0);
        rec.GetLength() = reclens.back();
        rec.GetRecordId().sid = INVALID_SID;
        ASSERT_NO_ERROR((void)dp.InsertRecord(rec));
        if (rec.GetRecordId().sid == INVALID_SID) {
            break;
        }
    }

    ASSERT_FATAL_ERROR(dp.ShiftSlots(1, false));

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
    BonusTestSortedDataPage,
    ::testing::Combine(
        ::testing::ValuesIn(s_test_reclens),
        ::testing::ValuesIn(s_test_usr_data_sz)),
    [](const ::testing::TestParamInfo<BonusTestSortedDataPage::ParamType>& info) {
        return absl::StrCat(
            std::get<0>(info.param).first,
            "_",
            std::get<0>(info.param).second,
            "_",
            std::get<1>(info.param));
    }
);

}   // namespace taco
