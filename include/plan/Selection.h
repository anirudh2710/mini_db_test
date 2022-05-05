#ifndef PLAN_SELECTION_H
#define PLAN_SELECTION_H

#include "tdb.h"
#include "catalog/systables/Type.h"

#include "plan/PlanNode.h"
#include "expr/ExprNode.h"

namespace taco {

class SelectionState;

/*!
 * `Selection` is the physical plan for selecitons. This physical plan should
 * remember its child plan and a boolean expression as its selection condition.
 * The output schema of a selection plan should always be the same as its
 * child.
 */
class Selection: public PlanNode {
public:
    static std::unique_ptr<Selection>
    Create(std::unique_ptr<PlanNode>&& child,
           std::unique_ptr<ExprNode>&& cond);

    ~Selection() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    Selection(std::unique_ptr<PlanNode>&& child, std::unique_ptr<ExprNode>&& cond);

    std::unique_ptr<ExprNode> m_selectexpr;

    std::unique_ptr<Schema> m_selectschema;

    friend class SelectionState;
};

}    // namespace taco

#endif
