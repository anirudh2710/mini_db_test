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
    // TODO implement it
    return false;
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
