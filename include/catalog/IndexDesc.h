#ifndef CATALOG_INDEXDESC_H
#define CATALOG_INDEXDESC_H

#include "tdb.h"

#include "catalog/Schema.h"
#include "catalog/systables.h"

namespace taco {

class IndexDesc {
public:
    /*!
     * This overload simply passes the arguments to the constructor.
     */
    static IndexDesc *Create(std::shared_ptr<const SysTable_Index> index,
            std::vector<std::shared_ptr<SysTable_IndexColumn>> index_columns,
            std::unique_ptr<Schema> key_schema);

    /*!
     * Returns the cached Index entry associated with this index descriptor.
     * Note that the Index entry may be destructed as well if the index
     * descriptor is destructed, so you must access the Index entry while the
     * index descriptor is still alive.
     */
    constexpr const SysTable_Index *
    GetIndexEntry() const {
        return m_index.get();
    }

    constexpr const SysTable_IndexColumn *
    GetIndexColumnEntry(FieldId keyid) const {
        return m_index_columns[keyid].get();
    }

    /*!
     * Returns the cached index key schema, which we guanrantee
     * `IsLayoutComputed() == true'.
     */
    constexpr const Schema*
    GetKeySchema() const {
        return m_key_schema.get();
    }


private:
    IndexDesc(std::shared_ptr<const SysTable_Index> index,
        std::vector<std::shared_ptr<SysTable_IndexColumn>> index_columns,
        std::unique_ptr<Schema> key_schema):
        m_index(std::move(index)),
        m_index_columns(std::move(index_columns)),
        m_key_schema(std::move(key_schema)) {}

    std::shared_ptr<const SysTable_Index> m_index;
    std::vector<std::shared_ptr<SysTable_IndexColumn>> m_index_columns;
    std::unique_ptr<Schema> m_key_schema;
};


}   // namespace taco

#endif  // CATALOG_INDEXDESC_H
