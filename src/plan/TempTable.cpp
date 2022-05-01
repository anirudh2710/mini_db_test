#include "plan/TempTable.h"
#include "execution/TempTableState.h"

namespace taco {

TempTable::TempTable(const Schema* schema)
: PlanNode(TAG(TempTable))
{
    m_schema = absl::WrapUnique(new Schema(*schema));
    m_schema->ComputeLayout();
    m_data.clear();
}

std::unique_ptr<TempTable>
TempTable::Create(const Schema* schema) {
    return absl::WrapUnique(new TempTable(schema));
}

TempTable::~TempTable() {
}

void
TempTable::insert_record(std::vector<Datum>&& record) {
    m_data.emplace_back(std::move(record));
}

void
TempTable::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
TempTable::create_exec_state() const {
    return absl::WrapUnique(new TempTableState(this));
}

const Schema*
TempTable::get_output_schema() const {
    return m_schema.get();
}

}
