#include "plan/IndexNestedLoop.h"

#include "expr/optypes.h"
#include "catalog/CatCache.h"
#include "execution/IndexNestedLoopState.h"
#include "utils/builtin_funcs.h"

namespace taco {

std::unique_ptr<IndexNestedLoop>
IndexNestedLoop::Create(
    std::unique_ptr<PlanNode>&& outer,
    std::shared_ptr<const IndexDesc> inner_idxdesc,
    std::vector<std::unique_ptr<ExprNode>>&& joinexprs_outer,
    std::unique_ptr<ExprNode>&& upper_expr,
    bool lower_isstrict,
    bool upper_isstrict) {
    // TODO implement it
    return nullptr;
}

IndexNestedLoop::~IndexNestedLoop() {}

void
IndexNestedLoop::node_properties_to_string(std::string &buf, int indent) const {
    // optionally implement it for debugging
}

std::unique_ptr<PlanExecState>
IndexNestedLoop::create_exec_state() const {
    return nullptr;
}

const Schema *
IndexNestedLoop::get_output_schema() const {
    return nullptr;
}

}   // namespace taco

