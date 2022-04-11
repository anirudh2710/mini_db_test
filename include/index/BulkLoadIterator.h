#ifndef INDEX_BULKLOADITERATOR_H
#define INDEX_BULKLOADITERATOR_H

#include "tdb.h"

#include "catalog/Schema.h"
#include "index/IndexKey.h"
#include "storage/Record.h"

namespace taco {

/*!
 * BulkLoadIterator is an interface for providing (key, RecordId) pairs for
 * index bulk loading. Different use cases may implement the Next() virtual
 * function differently to iterate and construct the keys and record ids.
 */
class BulkLoadIterator {
public:
    /*!
     * Constructs a bulk load iterator with the \p key_schema. The \p
     * key_schema must remain alive for the duration that the bulk load
     * iterator is alive.
     */
    BulkLoadIterator(const Schema *key_schema):
        m_key_schema(key_schema),
        m_key(unique_aligned_alloc(8,
                IndexKey::ComputeStructSize(m_key_schema->GetNumFields()))),
        m_recid() {}

    /*!
     * Destructor.
     */
    virtual ~BulkLoadIterator() {}

    /*!
     * Subclass should override the Next() function for moving to the next
     * (key, record id) pair. Upon a successful return, Next() should call
     * IndexKey::Construct on the preallocated IndexKey \p m_key, which is
     * sufficient to hold `key_schema->GetNumFields()` references to the keys,
     * and \p m_recid is set record id this key maps to.
     */
    virtual bool Next() = 0;

    /*!
     * Returns the current key. Undefined if Next() has not been called
     * or a previous call returns `false`.
     */
    const IndexKey *GetCurrentKey() {
        return reinterpret_cast<IndexKey*>(m_key.get());
    }

    /*!
     * Returns the current record id. Undefined if Next() has not been called
     * or a previous call returns `false`.
     */
    const RecordId &GetCurrentRecordId() {
        return m_recid;
    }

    /*!
     * Ends the scan. If the subclass does want it to be called again
     * in the destructor if it has been called,  it should set m_key_schema
     * to \p nullptr.
     */
    virtual void EndScan() = 0;

protected:
    const Schema *m_key_schema;
    unique_malloced_ptr m_key;
    RecordId m_recid;
};

}   // namespace taco

#endif      // INDEX_BULKLOADITERATOR_H
