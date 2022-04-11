#define DEFAULT_BUFPOOL_SIZE 100
#include "index/btree/TestBTree.h"

namespace taco {

using BasicTestBTreeUtils = TestBTree;

TEST_F(BasicTestBTreeUtils, TestCreateNewBTreePage) {
    TDB_TEST_BEGIN

    ScopedBufferId bufid = CreateNewBTreePage(m_idx_2, true, true, 1, 2);
    VarlenDataPage dp = g_bufman->GetBuffer(bufid);
    auto hdr = (BTreePageHeaderData*) dp.GetUserData();
    EXPECT_EQ(hdr->m_flags, BTREE_PAGE_ISROOT | BTREE_PAGE_ISLEAF);
    EXPECT_EQ(hdr->m_prev_pid, 1);
    EXPECT_EQ(hdr->m_next_pid, 2);
    EXPECT_EQ(hdr->m_totrlen, 0);

    bufid = CreateNewBTreePage(m_idx_2, true, false, 1, 3);
    dp = g_bufman->GetBuffer(bufid);
    hdr = (BTreePageHeaderData*) dp.GetUserData();
    EXPECT_EQ(hdr->m_flags, BTREE_PAGE_ISROOT);
    EXPECT_EQ(hdr->m_prev_pid, 1);
    EXPECT_EQ(hdr->m_next_pid, 3);
    EXPECT_EQ(hdr->m_totrlen, 0);

    bufid = CreateNewBTreePage(m_idx_2, false, true, 2, 3);
    dp = g_bufman->GetBuffer(bufid);
    hdr = (BTreePageHeaderData*) dp.GetUserData();
    EXPECT_EQ(hdr->m_flags, BTREE_PAGE_ISLEAF);
    EXPECT_EQ(hdr->m_prev_pid, 2);
    EXPECT_EQ(hdr->m_next_pid, 3);
    EXPECT_EQ(hdr->m_totrlen, 0);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeUtils, TestGetBTreeMetaPage) {
    TDB_TEST_BEGIN

    auto f = g_fileman->Open(m_idxdesc_2->GetIndexEntry()->idxfid());

    ScopedBufferId bufid = GetBTreeMetaPage(m_idx_2);
    EXPECT_EQ(g_bufman->GetPageNumber(bufid), f->GetFirstPageNumber());
    auto meta = (BTreeMetaPageData*) g_bufman->GetBuffer(bufid);
    EXPECT_NE(meta->m_root_pid, InvalidOid);

    ScopedBufferId root_bufid;
    char *root_buf;
    ASSERT_NO_ERROR(
        root_bufid = g_bufman->PinPage(meta->m_root_pid, &root_buf));
    VarlenDataPage root_pg(root_buf);
    auto root_hdr = (BTreePageHeaderData*) root_pg.GetUserData();
    EXPECT_LT(root_pg.GetMaxSlotId(), root_pg.GetMinSlotId());
    EXPECT_TRUE(root_hdr->IsRootPage());
    EXPECT_TRUE(root_hdr->IsLeafPage());
    EXPECT_EQ(root_hdr->m_totrlen, 0);
    EXPECT_EQ(root_hdr->m_prev_pid, InvalidOid);
    EXPECT_EQ(root_hdr->m_next_pid, InvalidOid);

    TDB_TEST_END
}

TEST_F(BasicTestBTreeUtils, TestCreateBTreeRecords) {
    TDB_TEST_BEGIN

    RecordId recid{100, 5};

    {
        maxaligned_char_buf buf(1000, 0);
        IndexKey *key = get_key_f1(200);
        ASSERT_NO_ERROR(CreateLeafRecord(m_idx_1, key, recid, buf));
        ASSERT_EQ(buf.size(), BTreeLeafRecordHeaderSize + s_f1_len);
        auto hdr = (BTreeLeafRecordHeaderData*) buf.data();
        ASSERT_EQ(hdr->m_recid, recid);
        ASSERT_EQ(read_f1_payload(hdr->GetPayload()), get_ikey(200));

        maxaligned_char_buf buf2(1000, 0);
        Record rec(buf);
        ASSERT_NO_ERROR(CreateInternalRecord(m_idx_1, rec, 102, true, buf2));
        ASSERT_EQ(buf2.size(), BTreeInternalRecordHeaderSize + s_f1_len);
        auto hdr2 = (BTreeInternalRecordHeaderData*) buf2.data();
        ASSERT_EQ(hdr2->m_child_pid, 102);
        ASSERT_EQ(hdr2->m_heap_recid, recid);
        ASSERT_EQ(read_f1_payload(hdr->GetPayload()), get_ikey(200));

        maxaligned_char_buf buf3(1000, 0);
        Record rec2(buf2);
        ASSERT_NO_ERROR(CreateInternalRecord(m_idx_1, rec2, 107, false, buf3));
        ASSERT_EQ(buf3.size(), BTreeInternalRecordHeaderSize + s_f1_len);
        auto hdr3 = (BTreeInternalRecordHeaderData*) buf3.data();
        ASSERT_EQ(hdr3->m_child_pid, 107);
        ASSERT_EQ(hdr3->m_heap_recid, recid);
        ASSERT_EQ(read_f1_payload(hdr->GetPayload()), get_ikey(200));
    }

    {
        maxaligned_char_buf buf(1000, 0);
        IndexKey *key = get_key_f2(200);
        ASSERT_NO_ERROR(CreateLeafRecord(m_idx_2, key, recid, buf));
        ASSERT_EQ(buf.size(), BTreeLeafRecordHeaderSize + s_f2_len);
        auto hdr = (BTreeLeafRecordHeaderData*) buf.data();
        ASSERT_EQ(hdr->m_recid, recid);
        ASSERT_EQ(read_f2_payload(hdr->GetPayload()), get_skey(200));

        maxaligned_char_buf buf2(1000, 0);
        Record rec(buf);
        ASSERT_NO_ERROR(CreateInternalRecord(m_idx_2, rec, 102, true, buf2));
        ASSERT_EQ(buf2.size(), BTreeInternalRecordHeaderSize + s_f2_len);
        auto hdr2 = (BTreeInternalRecordHeaderData*) buf2.data();
        ASSERT_EQ(hdr2->m_child_pid, 102);
        ASSERT_EQ(hdr2->m_heap_recid, recid);
        ASSERT_EQ(read_f2_payload(hdr->GetPayload()), get_skey(200));

        maxaligned_char_buf buf3(1000, 0);
        Record rec2(buf2);
        ASSERT_NO_ERROR(CreateInternalRecord(m_idx_2, rec2, 107, false, buf3));
        ASSERT_EQ(buf3.size(), BTreeInternalRecordHeaderSize + s_f2_len);
        auto hdr3 = (BTreeInternalRecordHeaderData*) buf3.data();
        ASSERT_EQ(hdr3->m_child_pid, 107);
        ASSERT_EQ(hdr3->m_heap_recid, recid);
        ASSERT_EQ(read_f2_payload(hdr->GetPayload()), get_skey(200));
    }

    TDB_TEST_END
}

TEST_F(BasicTestBTreeUtils, TestBTreeIsEmpty) {
    TDB_TEST_BEGIN

    ASSERT_TRUE(m_idx_1->IsEmpty());

    auto fid = m_idx_1->GetIndexDesc()->GetIndexEntry()->idxfid();
    auto idxfile = g_fileman->Open(fid);
    char* buf;
    ScopedBufferId bufid = g_bufman->PinPage(idxfile->GetLastPageNumber(), &buf);

    VarlenDataPage pg(buf);
    maxaligned_char_buf recbuf(1000, 0);
    IndexKey *key = get_key_f2(200);
    RecordId recid{100, 3};
    ASSERT_NO_ERROR(CreateLeafRecord(m_idx_1, key, recid, recbuf));
    Record leafrec(recbuf);
    ASSERT_TRUE(pg.InsertRecord(leafrec));

    ASSERT_TRUE(!m_idx_1->IsEmpty());



    TDB_TEST_END
}


}    // namespace taco

