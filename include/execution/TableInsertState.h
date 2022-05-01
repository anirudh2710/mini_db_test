#ifndef EXECUTION_TABLEINSERTSTATE_H
#define EXECUTION_TABLEINSERTSTATE_H

#include "execution/PlanExecState.h"
#include "plan/TableInsert.h"
#include "index/Index.h"

namespace taco {

/*!
 * `TableDeleteState` is the execution state for table insertion action. This execution
 * state should only return one record with one integer column indicating how many rows
 * has been successfuly inserted by this action. Note that this operation cannot be rewinded.
 * Thus, you should throw an error if someone try calling `rewind` of a table insertion
 * execution state.
 *
 * A table access interface `Table` is passed as \p table when constructed. All the data
 * accesses and insertions to the underlying table should go through that access interface.
 * Besides, all the indexes associated with this table is also passed in so that they can
 * be updated correspondingly.
 *
 * CAUTION: You need to make sure the returning count's `NullableDatumRef` that the
 * exeuction state pointing at is always memory safe until the operator is closed.
 */
class TableInsertState: public PlanExecState {
public:
    ~TableInsertState() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    void init() override;

    bool next_tuple() override;

    std::vector<NullableDatumRef> get_record() override;

    void close() override;

    void rewind() override;

    const PlanNode*
    get_plan() const {
        return m_plan;
    }

private:
    TableInsertState(const TableInsert* plan,
                     std::unique_ptr<Table>&& table,
                     std::vector<std::unique_ptr<Index>>&& idxs,
                     std::unique_ptr<PlanExecState>&& child);

    const TableInsert* m_plan;

    std::unique_ptr<Table> m_table;
    std::vector<std::unique_ptr<Index>> m_idxs;
    int64_t m_res;

    friend class TableInsert;
};

}    // namespace taco

#endif
