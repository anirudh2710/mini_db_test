#include "plan/Selection.h"
#include "execution/SelectionState.h"
#include "catalog/systables/initoids.h"

#include <absl/strings/str_format.h>

namespace taco {

Selection::Selection(std::unique_ptr<PlanNode>&& child, std::unique_ptr<ExprNode>&& cond)
: PlanNode(TAG(Selection), std::move(child)), m_selectexpr(std::move(cond))
{
    // TODO: implement it
}

std::unique_ptr<Selection>
Selection::Create(std::unique_ptr<PlanNode>&& child, std::unique_ptr<ExprNode>&& cond) {
    return absl::WrapUnique(new Selection(std::move(child), std::move(cond)));
}

Selection::~Selection() {
    // TODO: implement it
}

void
Selection::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
Selection::create_exec_state() const {
    return absl::WrapUnique(new SelectionState(this,  get_input_as<PlanNode>(0)->create_exec_state()));
}

const Schema*
Selection::get_output_schema() const {
    // TODO: implement it
    return get_input_as<PlanNode>(0)->get_output_schema();
}


}