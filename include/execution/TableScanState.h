#ifndef EXECUTION_TABLESCANSTATE_H
#define EXECUTION_TABLESCANSTATE_H

#include "tdb.h"

#include "execution/PlanExecState.h"
#include "plan/TableScan.h"
#include "storage/Table.h"

namespace taco {

/*!
 * `TableScanState` is the execution state for heap file table scan. This execution
 * state should return all heap file records of the table.
 *
 * A table access interface `Table` is passed as \p table when constructed. All the data
 * accesses to the underlying table should go through that access interface.
 *
 * CAUTION: You need to make sure the output record's `NullableDatumRef` are always memory
 * safe at any time until the operator is closed.
 */
class TableScanState: public PlanExecState {
public:
    ~TableScanState() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

    void init() override;

    bool next_tuple() override;

    std::vector<NullableDatumRef> get_record() override;

    void close() override;

    void rewind() override;

    Datum save_position() const override;

    bool rewind(DatumRef saved_position) override;

    const PlanNode*
    get_plan() const {
        return m_plan;
    }

private:
    TableScanState(const TableScan *plan, std::unique_ptr<Table> table);

    const TableScan *m_plan;

    bool m_extracted;
    bool m_at_end;
    std::unique_ptr<Table> m_table;
    Table::Iterator m_tabiter;
    std::vector<Datum> m_fields;

    friend class TableScan;
};

}    // namespace taco

#endif
