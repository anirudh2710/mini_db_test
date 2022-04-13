#include "plan/Projection.h"
#include "execution/ProjectionState.h"

namespace taco {

Projection::Projection(std::unique_ptr<PlanNode>&& child,
                       std::vector<std::unique_ptr<ExprNode>>&& exprs)
: PlanNode(TAG(Projection), std::move(child))
, m_exprs(std::move(exprs))
{
    std::vector<Oid> types;
    for (size_t i = 0; i < m_exprs.size(); ++i) {
        types.emplace_back(m_exprs[i]->ReturnType()->typid());
    }
    std::vector<uint64_t> typparams(m_exprs.size(), 0);
    std::vector<bool> nullables(m_exprs.size(), false);

    m_schema = absl::WrapUnique(Schema::Create(types, typparams, nullables));
    m_schema->ComputeLayout();
}

std::unique_ptr<Projection>
Projection::Create(std::unique_ptr<PlanNode>&& child,
                   std::vector<std::unique_ptr<ExprNode>>&& exprs) {

    return absl::WrapUnique(new Projection(std::move(child), std::move(exprs)));
}

Projection::~Projection() {
}

void
Projection::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState> Projection::create_exec_state() const {
    auto child_exec_state = get_input_as<PlanNode>(0)->create_exec_state();
    return absl::WrapUnique(new ProjectionState(this, std::move(child_exec_state)));
}

const Schema*
Projection::get_output_schema() const {
    return m_schema.get();
}



}
