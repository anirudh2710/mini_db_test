#include "catalog/CatCache.h"

#include "plan/TableDelete.h"
#include "execution/TableDeleteState.h"
#include "storage/Table.h"
#include "index/Index.h"

namespace taco {

TableDelete::TableDelete(std::shared_ptr<const TableDesc> tabdesc,
                         std::unique_ptr<ExprNode>&& cond)
: PlanNode(TAG(TableDelete))
, m_tabdesc(tabdesc), m_cond(std::move(cond)) {}

std::unique_ptr<TableDelete>
TableDelete::Create(std::shared_ptr<const TableDesc> tabdesc,
                    std::unique_ptr<ExprNode>&& cond) {
    return absl::WrapUnique(new TableDelete(tabdesc, std::move(cond)));
}

TableDelete::~TableDelete() {
}

void
TableDelete::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
TableDelete::create_exec_state() const {
    auto tbl = Table::Create(m_tabdesc);
    auto idx_oids = g_catcache->FindAllIndexesOfTable(m_tabdesc->GetTableEntry()->tabid());
    std::vector<std::unique_ptr<Index>> idxs;
    for (auto x: idx_oids) {
        idxs.emplace_back(Index::Create(g_catcache->FindIndexDesc(x)));
    }
    return absl::WrapUnique(new TableDeleteState(this, std::move(tbl), std::move(idxs)));
}

const Schema*
TableDelete::get_output_schema() const {
    return g_actsch;
}

}
