#include "index/Index.h"

#include "catalog/TableDesc.h"
#include "catalog/CatCache.h"
#include "index/volatiletree/VolatileTree.h"
#include "index/btree/BTree.h"

namespace taco {

Index::Index(std::shared_ptr<const IndexDesc> idxdesc):
    m_idxdesc(std::move(idxdesc)),
    m_key() {}

Index::~Index() {}

void
Index::BulkLoad(BulkLoadIterator &iter) {
    while (iter.Next()) {
        this->InsertKey(iter.GetCurrentKey(), iter.GetCurrentRecordId());
    }
    iter.EndScan();
}

void
Index::Initialize(const IndexDesc *idxdesc) {
    IdxType idxtyp = idxdesc->GetIndexEntry()->idxtyp();
    switch (idxtyp) {
    case IDXTYP(VOLATILETREE):
        VolatileTree::Initialize(idxdesc);
        break;
    case IDXTYP(BTREE):
        BTree::Initialize(idxdesc);
        break;
    default:
        LOG(kFatal, "unrecognized index type: %d", (int) idxtyp);
    }
}

std::unique_ptr<Index>
Index::Create(std::shared_ptr<const IndexDesc> idxdesc) {
    IdxType idxtyp = idxdesc->GetIndexEntry()->idxtyp();
    switch (idxtyp) {
    case IDXTYP(VOLATILETREE):
        return VolatileTree::Create(std::move(idxdesc));
    case IDXTYP(BTREE):
        return BTree::Create(std::move(idxdesc));
    }

    LOG(kFatal, "unrecognized index type: %d", (int) idxtyp);
    return nullptr;
}

bool
Index::InsertRecord(const Record &rec, const Schema *tabschema) {
    std::vector<Datum> data;
    PrepareKey(data, rec.GetData(), tabschema);
    return InsertKey((IndexKey*)m_key.get(), rec.GetRecordId());
}

bool
Index::DeleteRecord(Record &rec, const Schema *tabschema) {
    std::vector<Datum> data;
    PrepareKey(data, rec.GetData(), tabschema);
    return DeleteKey((IndexKey*)m_key.get(), rec.GetRecordId());
}

void
Index::PrepareKey(std::vector<Datum> &data,
                  const char *recbuf,
                  const Schema *tabschema) {
    ASSERT(data.empty());
    if (!m_key.get()) {
        m_key = unique_aligned_alloc(8,
            IndexKey::ComputeStructSize(GetKeySchema()->GetNumFields()));
    }

    std::shared_ptr<const TableDesc> tabdesc;
    if  (!tabschema) {
        tabdesc = g_catcache->FindTableDesc(
            GetIndexDesc()->GetIndexEntry()->idxtabid());
        tabschema = tabdesc->GetSchema();
    }

    for (FieldId i = 0; i < GetKeySchema()->GetNumFields(); ++i) {
        FieldId idxcoltabcolid =
            GetIndexDesc()->GetIndexColumnEntry(i)->idxcoltabcolid();
        ASSERT(idxcoltabcolid != InvalidFieldId);
        data.emplace_back(tabschema->GetField(idxcoltabcolid, recbuf));
    }
    IndexKey::Construct(m_key.get(), data);
}

}   // namespace taco

