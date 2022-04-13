#ifndef CATALOG_TABLE_DESC_H
#define CATALOG_TABLE_DESC_H

#include "tdb.h"

#include "catalog/Schema.h"
#include "catalog/systables.h"

namespace taco {

class TableDesc {
public:
    /*!
     * Creates a new table descriptor by building the schema object constructed
     * from the columns (which performs lookups in the global catalog cache
     * instance `g_catcache`).
     */
    static TableDesc *Create(std::shared_ptr<const SysTable_Table> table,
                             const std::vector<const SysTable_Column*> &columns);

    //! This overload simply passes the arguments to the constructor.
    static TableDesc *Create(std::shared_ptr<const SysTable_Table> table,
                             std::unique_ptr<Schema> schema);

    /*!
     * Returns the cached Table entry associated with this table descriptor.
     * Note that the table entry may be destructed as well if the table
     * descriptor is destructed, so you must access the table entry while the
     * table descriptor is still alive.
     */
    constexpr const SysTable_Table *
    GetTableEntry() const {
        return m_table.get();
    }

    /*!
     * Returns the cached table schema, which we guanrantee `IsLayoutComputed()
     * == true'.
     */
    constexpr const Schema*
    GetSchema() const {
        return m_schema.get();
    }

private:
    TableDesc(std::shared_ptr<const SysTable_Table> table,
              std::unique_ptr<Schema> schema):
        m_table(std::move(table)),
        m_schema(std::move(schema)) {}

    std::shared_ptr<const SysTable_Table> m_table;
    std::unique_ptr<Schema> m_schema;
};

}   // namespace taco

#endif      // CATALOG_TABLE_DESC_H
