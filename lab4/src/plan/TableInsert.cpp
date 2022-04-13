#include "catalog/CatCache.h"

#include "plan/TableInsert.h"
#include "execution/TableInsertState.h"
#include "index/Index.h"
#include "storage/Table.h"

namespace taco {

TableInsert::TableInsert(std::unique_ptr<PlanNode>&& child,
                         std::shared_ptr<const TableDesc> tabdesc)
: PlanNode(TAG(TableInsert), std::move(child))
, m_tabdesc(tabdesc) {
    if (!Schema::Compatible(tabdesc->GetSchema(),
                            get_child(0)->get_output_schema())) {
        LOG(kError,
            "The schema of inserting table and child plan for table insert are not compatible");
    }
}

std::unique_ptr<TableInsert>
TableInsert::Create(std::unique_ptr<PlanNode>&& child,
                    std::shared_ptr<const TableDesc> tabdesc) {
    return absl::WrapUnique(new TableInsert(std::move(child), tabdesc));
}

TableInsert::~TableInsert() {
}

void
TableInsert::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
TableInsert::create_exec_state() const {
    auto child = get_child(0)->create_exec_state();
    auto table = Table::Create(m_tabdesc);
    auto idx_oids = g_catcache->FindAllIndexesOfTable(m_tabdesc->GetTableEntry()->tabid());
    std::vector<std::unique_ptr<Index>> idxs;
    for (auto x: idx_oids) {
        idxs.emplace_back(Index::Create(g_catcache->FindIndexDesc(x)));
    }
    return absl::WrapUnique(new TableInsertState(this, std::move(table), std::move(idxs), std::move(child)));
}

const Schema*
TableInsert::get_output_schema() const {
    return g_actsch;
}

}
