#ifndef PLAN_SORT_H
#define PLAN_SORT_H

#include "plan/PlanNode.h"
#include "expr/ExprNode.h"
#include "extsort/ExternalSort.h"

namespace taco {

class SortState;

/*!
 * `Sort` is the physical plan for sorting operators. This physical plan should
 * remember its child plan, a list of expressions based on which the records
 * should be sorted, and whether the output should be in ascending or descending
 * order for each expression. Each sorting expression should be evaluated on input
 * columns from the child plan and does not have to be a particular column.
 */
class Sort: public PlanNode {
public:
    static std::unique_ptr<Sort>
    Create(std::unique_ptr<PlanNode>&& child,
           std::vector<std::unique_ptr<ExprNode>>&& sort_exprs,
           const std::vector<bool>& desc);

    ~Sort() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    Sort(std::unique_ptr<PlanNode>&& child,
         std::vector<std::unique_ptr<ExprNode>>&& sort_exprs,
         const std::vector<bool>& desc);

    int sort_compare_func_impl(const char *a, const char *b) const;

    SortCompareFunction m_cmp;

    // You can add your own states here.

    friend class SortState;
};

};    // namespace taco

#endif
