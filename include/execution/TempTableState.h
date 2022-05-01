#ifndef EXECUTION_TEMPTABLESTATE_H
#define EXECUTION_TEMPTABLESTATE_H

#include "execution/PlanExecState.h"
#include "plan/TempTable.h"

namespace taco {

/*!
 * `TempTableState` is the execution state for a in-memory temp table scan. This
 * execution state should return all records in the in-memory temp table.
 *
 * CAUTION: You need to make sure the output record's `NullableDatumRef` are always
 * memory safe at any time until the operator is closed.
 */
class TempTableState: public PlanExecState {
public:
    ~TempTableState() override;

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

    Datum save_position() const override;

    bool rewind(DatumRef saved_position) override;

private:
    TempTableState(const TempTable* plan);

    const TempTable* m_plan;

    int64_t m_pos;

    friend class TempTable;
};

}    // namespace taco

#endif
