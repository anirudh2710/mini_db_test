#ifndef CATALOG_BOOTSTRAPCATCACHE_H
#define CATALOG_BOOTSTRAPCATCACHE_H

#include "tdb.h"

#include <absl/container/flat_hash_map.h>

#include "catalog/systables.h"
#include "catalog/TableDesc.h"
#include "catalog/systables/bootstrap_data.h"

namespace taco {

/*!
 * BootstrapCatCache stores hard-coded data needed to bootstrap the entire
 * database catalog. All other catelog cache will reference this class to load
 * the data needed.
 *
 *
 */
class BootstrapCatCache {
public:
    /*!
     * Initializes the data in the bootstrap systables. Must Call
     * BootstrapCatCache()::init() to finish initialization. The implementation
     * of this function is in catalog/systables/bootstrap_data.cpp, which is
     * automatically generated from the systable.
     */
    BootstrapCatCache();

    ~BootstrapCatCache();

    /*!
     * Finishes the initialization of the bootstrap catalog cache. Never call
     * this more than once.
     */
    void Init();

    const SysTable_Table *FindTable(Oid oid);

    const SysTable_Type *FindType(Oid oid);

    std::vector<const SysTable_Column*> FindColumns(Oid tabid);

    const TableDesc *FindTableDesc(Oid oid);

    const SysTable_Function *FindFunction(Oid oid);

private:
    /*!
     * Creates a TableDesc object for a systable table that consists of
     * the ``columns'', and insert it into this catalog.
     */
    void CreateTableDesc(const std::vector<const SysTable_Column*> &columns);

    SysTable_Table  m_table[num_bootstrap_tables];
    SysTable_Type   m_type[num_bootstrap_types];
    SysTable_Column m_column[num_bootstrap_columns];
    SysTable_Function m_function[num_bootstrap_functions];

    absl::flat_hash_map<Oid, const SysTable_Table*> m_tabid_lookup_table;
    absl::flat_hash_map<Oid, const SysTable_Type*> m_typid_lookup_table;
    absl::flat_hash_map<Oid, const SysTable_Function*> m_funcid_lookup_table;

    std::vector<std::unique_ptr<TableDesc>> m_table_desc;
    absl::flat_hash_map<Oid, const TableDesc *> m_table_desc_lookup_table;

};

}       // namespace taco

#endif  // CATALOG_BOOTSTRAP_CATCACHE_H
