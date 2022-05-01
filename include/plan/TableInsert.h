#ifndef PLAN_TABLEINSERT_H
#define PLAN_TABLEINSERT_H

#include "plan/PlanNode.h"
#include "catalog/TableDesc.h"

namespace taco {

class TableInsertState;

/*!
 * `TableInsert` is the physical plan for insert actions on a table. This physical
 * plan should remember which table we are inserting records to and a child plan
 * from which we get our new records. The output schema of this plan should be just
 * contain one INT8 column. There is a global schema you can use directly for this
 * purpose, which can be retrived through `g_actsch`.
 *
 * Note that the schema of inserting table **MUST** be compatible the output schema
 * of the child plan. If not, you should report an error. You can check if two schema
 * are union compatible through `Schema::Compatible()`.
 */
class TableInsert: public PlanNode {
public:
    static std::unique_ptr<TableInsert>
    Create(std::unique_ptr<PlanNode>&& child, std::shared_ptr<const TableDesc> tabdesc);

    ~TableInsert() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    TableInsert(std::unique_ptr<PlanNode>&& child, std::shared_ptr<const TableDesc> tabdesc);

    std::shared_ptr<const TableDesc> m_tabdesc;

    friend class TableInsertState;
};

}    // namespace taco

#endif
