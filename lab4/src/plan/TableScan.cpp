#include "plan/TableScan.h"
#include "execution/TableScanState.h"

#include <absl/strings/str_format.h>

namespace taco {

TableScan::TableScan(std::shared_ptr<const TableDesc> tabdesc)
: PlanNode(TAG(TableScan))
, m_tabdesc(std::move(tabdesc)) {
}

std::unique_ptr<TableScan>
TableScan::Create(std::shared_ptr<const TableDesc> tabdesc) {
    ASSERT(tabdesc.get());
    return absl::WrapUnique(new TableScan(std::move(tabdesc)));
}

TableScan::~TableScan() {
}

void
TableScan::node_properties_to_string(std::string &buf, int indent) const {
    append_indent(buf, indent);
    absl::StrAppendFormat(&buf,
                          "tabid = " OID_FORMAT,
                          m_tabdesc->GetTableEntry()->tabid());
}

std::unique_ptr<PlanExecState>
TableScan::create_exec_state() const {
    std::unique_ptr<Table> table = Table::Create(m_tabdesc);
    ASSERT(table.get());
    return absl::WrapUnique(new TableScanState(this, std::move(table)));
}

const Schema *
TableScan::get_output_schema() const {
    return m_tabdesc->GetSchema();
}

}    // namespace taco
