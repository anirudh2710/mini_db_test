#include "catalog/IndexDesc.h"

namespace taco {

IndexDesc*
IndexDesc::Create(std::shared_ptr<const SysTable_Index> index,
    std::vector<std::shared_ptr<SysTable_IndexColumn>> index_columns,
    std::unique_ptr<Schema> key_schema) {
    return new IndexDesc(std::move(index), std::move(index_columns),
                         std::move(key_schema));
}

}   // namespace taco
