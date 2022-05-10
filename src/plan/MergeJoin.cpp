#include "plan/MergeJoin.h"
#include "execution/MergeJoinState.h"
#include "catalog/CatCache.h"
#include "expr/optypes.h"
#include "utils/builtin_funcs.h"

namespace taco {

std::unique_ptr<MergeJoin>
MergeJoin::Create(std::unique_ptr<PlanNode> &&outer,
                  std::unique_ptr<PlanNode> &&inner,
                  std::vector<std::unique_ptr<ExprNode>> &&joinexprs_outer,
                  std::vector<std::unique_ptr<ExprNode>> &&joinexprs_inner) {
    // TODO implement it
    return nullptr;
}

MergeJoin::~MergeJoin() {
}

void
MergeJoin::node_properties_to_string(std::string &buf, int indent) const {
    // optionally implement it for debugging
}

std::unique_ptr<PlanExecState>
MergeJoin::create_exec_state() const {
    // TODO implement it
    return nullptr;
}

const Schema*
MergeJoin::get_output_schema() const {
    // TODO implement it
    return nullptr;
}

}   // namespace taco

