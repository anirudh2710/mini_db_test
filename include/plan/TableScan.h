#ifndef PLAN_TABLESCAN_H
#define PLAN_TABLESCAN_H

#include "tdb.h"

#include "catalog/TableDesc.h"
#include "plan/PlanNode.h"

namespace taco {

/*!
 * `TableScan` is the physical plan for heap file table scanning. This physical
 * plan should remember which table it is scanning.
 *
 * Note that the output schema for this physical plan should be exactly the schema
 * of scanning table.
 */
class TableScan: public PlanNode {
public:
    static std::unique_ptr<TableScan>
    Create(std::shared_ptr<const TableDesc> tabdesc);

    ~TableScan() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    TableScan(std::shared_ptr<const TableDesc> tabdesc);


    std::shared_ptr<const TableDesc> m_tabdesc;

    // You can add your own states here.
};

}    // namespace taco

#endif
