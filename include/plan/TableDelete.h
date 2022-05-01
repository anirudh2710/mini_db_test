#ifndef PLAN_TABLEDELETE_H
#define PLAN_TABLEDELETE_H

#include "tdb.h"
#include "catalog/TableDesc.h"

#include "plan/PlanNode.h"
#include "expr/ExprNode.h"

namespace taco {

class TableDeleteState;

/*!
 * `TableDelete` is the physical plan for delete actions on a table. This physical
 * plan should remember which table we are deleting records from and an expression
 * based on which a record should be removed by this plan. The output schema of
 * this plan should be just contain one INT8 column. There is a global schema you
 * can use directly for this purpose, which can be retrived through `g_actsch`.
 */
class TableDelete: public PlanNode {
public:
    static std::unique_ptr<TableDelete>
    Create(std::shared_ptr<const TableDesc> tabdesc,
           std::unique_ptr<ExprNode>&& cond);

    ~TableDelete() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    TableDelete(std::shared_ptr<const TableDesc> tabdesc,
                std::unique_ptr<ExprNode>&& cond);

    std::shared_ptr<const TableDesc> m_tabdesc;
    std::unique_ptr<ExprNode> m_cond;

    friend class TableDeleteState;
};

}    // namespace taco

#endif
