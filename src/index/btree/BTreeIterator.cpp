#include "index/btree/BTreeInternal.h"
#include "index/tuple_compare.h"

namespace taco {

std::unique_ptr<Index::Iterator>
BTree::StartScan(const IndexKey *lower,
                 bool lower_isstrict,
                 const IndexKey *upper,
                 bool upper_isstrict) {
    ScopedBufferId leaf_bufid =
        FindLeafPage(lower,
                     lower_isstrict ? INFINITY_RECORDID : RecordId(),
                     nullptr);
    char *leaf_buf = g_bufman->GetBuffer(leaf_bufid);

    // the sid is one prior to the first potential key in the range
    SlotId sid;
    if (lower) {
        sid = BinarySearchOnPage(leaf_buf, lower,
            lower_isstrict ? INFINITY_RECORDID : RecordId());
    } else {
        VarlenDataPage pg(leaf_buf);
        sid = pg.GetMinSlotId() - 1;
    }

    return absl::WrapUnique(new Iterator(this, std::move(leaf_bufid), sid,
                                         upper, upper_isstrict));
}

BTree::Iterator::Iterator(BTree *btree,
                          ScopedBufferId bufid, SlotId one_before_matching_sid,
                          const IndexKey *upper, bool upper_isstrict):
    Index::Iterator(btree),
    m_bufid(std::move(bufid)),
    m_sid(one_before_matching_sid),
    m_rec(),
    m_upper((upper) ? upper->Copy() : nullptr),
    m_upper_data_buffer(),
    m_upper_isstrict(upper_isstrict) {

    // make sure we do not have dangling pointer to some deallocated Datum
    if (m_upper.get()) {
        m_upper->DeepCopy(btree->GetKeySchema(), m_upper_data_buffer);
    }
}


bool
BTree::Iterator::Next() {
    if (!m_bufid.IsValid()) {
        return false;
    }

    char *buf = g_bufman->GetBuffer(m_bufid);
    VarlenDataPage pg(buf);
    auto hdr = (BTreePageHeaderData*) pg.GetUserData();
    ++m_sid;
    while (m_sid > pg.GetMaxSlotId()) {
        // move to the next page
        PageNumber rsibling_pid = hdr->m_next_pid;
        g_bufman->UnpinPage(m_bufid);
        if (rsibling_pid == INVALID_PID) {
            return false;
        }
        m_bufid = g_bufman->PinPage(rsibling_pid, &buf);
        pg = VarlenDataPage(buf);
        m_sid = pg.GetMinSlotId();
    }

    // check if this record is still within our range
    BTree *btree = GetIndexAs<BTree>();
    Record rec = pg.GetRecord(m_sid);
    auto rechdr = (BTreeLeafRecordHeaderData*) rec.GetData();
    if (m_upper.get()) {
        // only need to compare the key portion
        int res = TupleCompare(m_upper.get(), rechdr->GetPayload(),
                               btree->GetKeySchema(),
                               btree->m_lt_funcs.data(),
                               btree->m_eq_funcs.data());
        if (res <= (m_upper_isstrict ? 0 : -1)) {
            // out of range, we're done
            g_bufman->UnpinPage(m_bufid);
            m_rec.Clear();
            return false;
        }
    }

    m_rec.GetData() = rechdr->GetPayload();
    m_rec.GetLength() = rec.GetLength() - BTreeLeafRecordHeaderSize;
    m_rec.GetRecordId() = rechdr->m_recid;
    return true;
}

bool
BTree::Iterator::IsAtValidItem() {
    return m_rec.IsValid();
}

const Record&
BTree::Iterator::GetCurrentItem() {
    return m_rec;
}

RecordId
BTree::Iterator::GetCurrentRecordId() {
    return m_rec.GetRecordId();
}

void
BTree::Iterator::EndScan() {
    if (m_bufid.IsValid()) {
        m_rec.Clear();
        m_bufid.Reset();
    }
}

}   // namespace taco
