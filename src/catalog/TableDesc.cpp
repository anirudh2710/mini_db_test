#include "catalog/TableDesc.h"

namespace taco {

TableDesc*
TableDesc::Create(std::shared_ptr<const SysTable_Table> table,
                  const std::vector<const SysTable_Column*> &columns) {
    if (!table || columns.empty())
        return nullptr;

    std::vector<Oid> typid;
    std::vector<uint64_t> typparam;
    std::vector<bool> isnullable;
    std::vector<std::string> field_names;
    typid.resize(columns.size());
    typparam.resize(columns.size());
    isnullable.resize(columns.size());
    field_names.resize(columns.size());
    for (const SysTable_Column *col : columns) {
        if (col->coltabid() != table->tabid())
            return nullptr;
        FieldId colid = col->colid();
        typid[colid] = col->coltypid();
        typparam[colid] = col->coltypparam();
        isnullable[colid] = col->colisnullable();
        field_names[colid] = col->colname();
    }

    std::unique_ptr<Schema> schema = absl::WrapUnique(
        Schema::Create(typid, typparam, isnullable, field_names));
    schema->ComputeLayout();

    return Create(std::move(table), std::move(schema));
}

TableDesc*
TableDesc::Create(std::shared_ptr<const SysTable_Table> table,
                  std::unique_ptr<Schema> schema) {
    return new TableDesc(std::move(table), std::move(schema));
}

}    // namespace taco
