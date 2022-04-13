#include "catalog/CatCache.h"
#include "plan/Aggregation.h"
#include "execution/AggregationState.h"

namespace taco {

Aggregation::Aggregation(std::unique_ptr<PlanNode>&& child,
                         std::vector<std::unique_ptr<ExprNode>>&& exprs,
                         const std::vector<AggType>& aggtyps)
: PlanNode(TAG(Aggregation), std::move(child))
{
    // TODO: implement it
}

Aggregation::~Aggregation() {
    // TODO: implement it
}

std::unique_ptr<Aggregation>
Aggregation::Create(std::unique_ptr<PlanNode>&& child,
                    std::vector<std::unique_ptr<ExprNode>>&& exprs,
                    const std::vector<AggType>& aggtyp) {
    return absl::WrapUnique(
        new Aggregation(std::move(child), std::move(exprs), aggtyp));
}

void
Aggregation::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
Aggregation::create_exec_state() const {
    // TODO: implement it
    return nullptr;
}

const Schema*
Aggregation::get_output_schema() const {
    // TODO: implement it
    return nullptr;
}

};
