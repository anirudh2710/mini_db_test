#include "catalog/BootstrapCatCache.h"

#include <algorithm>
#include <iterator>
#include <absl/memory/memory.h>

namespace taco {

BootstrapCatCache::~BootstrapCatCache() {}

void
BootstrapCatCache::Init() {
    for (int i = 0; i < num_bootstrap_tables; ++i) {
        auto res = m_tabid_lookup_table.try_emplace(
            m_table[i].tabid(), &m_table[i]);
        ASSERT(res.second);
    }

    for (int i = 0 ; i < num_bootstrap_types; ++i) {
        auto res = m_typid_lookup_table.try_emplace(
            m_type[i].typid(), &m_type[i]);
        ASSERT(res.second);
    }

    for (int i = 0; i < num_bootstrap_functions; ++i) {
        auto res = m_funcid_lookup_table.try_emplace(
            m_function[i].funcid(), &m_function[i]);
        ASSERT(res.second);
    }

    std::vector<const SysTable_Column*> sorted_columns;
    sorted_columns.reserve(num_bootstrap_columns);
    std::transform(m_column, m_column + num_bootstrap_columns,
        std::back_inserter(sorted_columns),
        [](SysTable_Column &col) -> const SysTable_Column* {
            return &col;
        });
    std::sort(sorted_columns.begin(), sorted_columns.end(),
        [](const SysTable_Column *col1, const SysTable_Column *col2) -> bool {
            return col1->coltabid() < col2->coltabid() ||
                (col1->coltabid() == col2->coltabid() &&
                 col1->colid() < col2->colid());
        });

    std::vector<const SysTable_Column*> table_columns;
    for (const SysTable_Column *col : sorted_columns) {
        if (!table_columns.empty() &&
            table_columns.front()->coltabid() != col->coltabid()) {
            CreateTableDesc(table_columns);
            table_columns.clear();
        }
        table_columns.push_back(col);
    }
    if (!table_columns.empty()) {
        CreateTableDesc(table_columns);
    }
}

const SysTable_Table*
BootstrapCatCache::FindTable(Oid oid) {
    auto iter = m_tabid_lookup_table.find(oid);
    if (iter == m_tabid_lookup_table.end())
        return nullptr;
    return iter->second;
}

const SysTable_Type *
BootstrapCatCache::FindType(Oid oid) {
    auto iter = m_typid_lookup_table.find(oid);
    if (iter == m_typid_lookup_table.end())
        return nullptr;
    return iter->second;
}

const SysTable_Function *
BootstrapCatCache::FindFunction(Oid oid) {
    auto iter = m_funcid_lookup_table.find(oid);
    if (iter == m_funcid_lookup_table.end())
        return nullptr;
    return iter->second;
}

std::vector<const SysTable_Column*>
BootstrapCatCache::FindColumns(Oid tabid) {
    std::vector<const SysTable_Column*> cols;
    for (int i = 0; i < num_bootstrap_columns; ++i) {
        if (m_column[i].coltabid() == tabid) {
            cols.push_back(&m_column[i]);
        }
    }
    return cols;
}

const TableDesc *
BootstrapCatCache::FindTableDesc(Oid oid) {
    auto iter = m_table_desc_lookup_table.find(oid);
    if (iter == m_table_desc_lookup_table.end()) {
        return nullptr;
    }
    return iter->second;
}

void
BootstrapCatCache::CreateTableDesc(
    const std::vector<const SysTable_Column*> &columns) {
    ASSERT(!columns.empty());
    const SysTable_Table *table = FindTable(columns.front()->coltabid());

    std::vector<Oid> typid;
    std::vector<uint64_t> typparam;
    std::vector<bool> isnullable;
    std::vector<std::string> field_names;
    typid.resize(columns.size());
    typparam.resize(columns.size());
    isnullable.resize(columns.size());
    field_names.resize(columns.size());
    for (const SysTable_Column *col : columns) {
        FieldId colid = col->colid();
        typid[colid] = col->coltypid();
        typparam[colid] = col->coltypparam();
        isnullable[colid] = col->colisnullable();
        field_names[colid] = col->colname();
    }

    std::unique_ptr<Schema> schema = absl::WrapUnique(
        Schema::Create(typid, typparam, isnullable, field_names));
    if (!schema)
        LOG(kError, "unable to create schema");
    schema->ComputeLayout(this);

    std::unique_ptr<TableDesc> table_desc = absl::WrapUnique(
        TableDesc::Create(
            // Since we require the TableDesc to shared a count of the
            // SysTable_Table struct but BootstrapCatCache does not allocate
            // these struct individually using new, we'd have to pass an
            // std::shared_ptr with a Deleter that does nothing when the
            // refcount drops to 0.
            std::shared_ptr<const SysTable_Table>(
                table, [](const SysTable_Table *) {}),
            std::move(schema)));
    if (!table_desc.get()) {
        LOG(kFatal,
            "unable to create a table descriptor for a bootstrap table "
            OID_FORMAT " (%s)",
            columns.front()->coltabid(),
            (table ? table->tabname().c_str() : "null"));
    }

    m_table_desc.emplace_back(std::move(table_desc));
    m_table_desc_lookup_table.try_emplace(
        table->tabid(), m_table_desc.back().get());
}


}    // namespace taco
