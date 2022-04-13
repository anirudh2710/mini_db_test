#ifndef PLAN_TEMPTABLE_H
#define PLAN_TEMPTABLE_H

#include "plan/PlanNode.h"
#include "catalog/Schema.h"

namespace taco {

class TempTableState;

/*!
 * `TempTable` is the physical plan for scanning on a temp in-memory table. This physical
 * plan should remember the schema of this temp table. When it finish construciton, it
 * should record an empty in-memory temp table with the given schema. If the user want
 * to insert a new record, s/he should do it through insert_record with a deserialized
 * record.
 */
class TempTable: public PlanNode {
public:
    static std::unique_ptr<TempTable>
    Create(const Schema* schema);

    ~TempTable() override;

    /*!
     * Move a fully constructed deserialized record into the in-memory temp table
     * this physical plan associate with.
     */
    void insert_record(std::vector<Datum>&& record);

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    TempTable(const Schema* schema);

    std::unique_ptr<Schema> m_schema;
    std::vector<std::vector<Datum>> m_data;

    friend class TempTableState;
};

}    // namespace taco

#endif
