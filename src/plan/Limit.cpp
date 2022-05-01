#include "plan/Limit.h"
#include "execution/LimitState.h"
#include "catalog/systables/initoids.h"

#include <absl/strings/str_format.h>

namespace taco {

Limit::Limit(std::unique_ptr<PlanNode>&& child, size_t cnt)
: PlanNode(TAG(Limit), std::move(child))
, m_cnt(cnt) {}

std::unique_ptr<Limit>
Limit::Create(std::unique_ptr<PlanNode>&& child, size_t cnt) {
    return absl::WrapUnique(new Limit(std::move(child), cnt));
}

Limit::~Limit() {
}

void
Limit::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
Limit::create_exec_state() const {
    return absl::WrapUnique(new LimitState(this, get_child(0)->create_exec_state()));
}

const Schema*
Limit::get_output_schema() const {
    return get_input_as<PlanNode>(0)->get_output_schema();
}


}
