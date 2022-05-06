#include "plan/Sort.h"
#include "catalog/CatCache.h"
#include "utils/builtin_funcs.h"
#include "expr/optypes.h"
#include "execution/SortState.h"

using namespace std::placeholders;

namespace taco {

Sort::Sort(std::unique_ptr<PlanNode>&& child,
           std::vector<std::unique_ptr<ExprNode>>&& sort_exprs,
           const std::vector<bool>& desc)
: PlanNode(TAG(Sort), std::move(child))
, m_cmp(std::bind(&Sort::sort_compare_func_impl, this, _1, _2))
{
    // TODO: implement it
}

int
Sort::sort_compare_func_impl(const char *a, const char *b) const {
    // TODO: implement it
    return 0;
}

std::unique_ptr<Sort>
Sort::Create(std::unique_ptr<PlanNode>&& child,
             std::vector<std::unique_ptr<ExprNode>>&& sort_exprs,
             const std::vector<bool>& desc) {
    return absl::WrapUnique(
        new Sort(std::move(child), std::move(sort_exprs), desc));
}

Sort::~Sort() {
    // TODO: implement it
}

void
Sort::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
Sort::create_exec_state() const {
    // TODO: implement it
    return absl::WrapUnique(new SortState(this, get_input_as<PlanNode>(0)->create_exec_state()));
}

const Schema*
Sort::get_output_schema() const {
    // TODO: implement it
    return nullptr;
}


}
