#include "index/TableBulkLoadIterator.h"

namespace taco {

TableBulkLoadIterator::TableBulkLoadIterator(
    std::shared_ptr<const IndexDesc> idxdesc,
    Table::Iterator &&iter):
        BulkLoadIterator(idxdesc->GetKeySchema()),
        m_idxdesc(std::move(idxdesc)),
        m_iter(std::move(iter)),
        m_data() {

    for (FieldId i = 0; i < m_key_schema->GetNumFields(); ++i) {
        ASSERT(m_idxdesc->GetIndexColumnEntry(i));
        ASSERT(m_idxdesc->GetIndexColumnEntry(i)->idxcolid() == i);
    }
}

bool
TableBulkLoadIterator::Next() {
    if (!m_iter.Next()) {
        if (m_recid.IsValid()) {
            m_recid.SetInvalid();
            m_data.clear();
            IndexKey::Construct(m_key.get(), m_data);
        }
        return false;
    }

    m_data.clear();
    const Record &rec = m_iter.GetCurrentRecord();
    const char *recbuf = rec.GetData();
    const Schema *tab_schema = m_iter.GetTable()->GetTableDesc()->GetSchema();
    for (FieldId i = 0; i < m_key_schema->GetNumFields(); ++i) {
        FieldId idxcoltabcolid =
            m_idxdesc->GetIndexColumnEntry(i)->idxcoltabcolid();

        // for now we assume all index columns map to some table column, so
        // idxcoltabcolid must be valid
        ASSERT(idxcoltabcolid != InvalidFieldId);

        m_data.emplace_back(tab_schema->GetField(idxcoltabcolid, recbuf));
    }
    IndexKey::Construct(m_key.get(), m_data);
    m_recid = rec.GetRecordId();
    return true;
}

void
TableBulkLoadIterator::EndScan() {
    m_iter.EndScan();
}

}   // namespace taco
