#ifndef INDEX_TABLEBULKLOADITERATOR_H
#define INDEX_TABLEBULKLOADITERATOR_H

#include "tdb.h"

#include "catalog/IndexDesc.h"
#include "index/BulkLoadIterator.h"
#include "storage/Table.h"

namespace taco {

class TableBulkLoadIterator: public BulkLoadIterator {
public:
    TableBulkLoadIterator(std::shared_ptr<const IndexDesc> idxdesc,
                          Table::Iterator &&iter);

    bool Next() override;

    void EndScan() override;

private:
    std::shared_ptr<const IndexDesc> m_idxdesc;
    Table::Iterator m_iter;
    std::vector<Datum> m_data;
};

}   // namespace taco

#endif  // INDEX_TABLEBULKLOADITERATOR_H
