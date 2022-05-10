#ifndef EXECUTION_MERGEJOINSTATE_H
#define EXECUTION_MERGEJOINSTATE_H

#include "tdb.h"

#include "execution/PlanExecState.h"
#include "plan/MergeJoin.h"

namespace taco {

/*!
 * `MergeJoinState` is the execution state for merge joins. This execution
 * state should be able to iterate through all join results in ascending order
 * with respect to join keys.
 *
 * You will rely on `save_position` and `rewind(saved_position)` calls of its
 * children. Thus, you need to implement these functions for all executions
 * states (including `MergeJoinState`) except for `TableInsert` and `TableDelete`.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that
 * the exeuction state pointing at is always memory safe until the operator is
 * closed.
 */
class MergeJoinState: public PlanExecState {
public:
    ~MergeJoinState() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

    void init() override;

    bool next_tuple() override;

    std::vector<NullableDatumRef> get_record() override;

    void close() override;

    void rewind() override;

    Datum save_position() const override;

    bool rewind(DatumRef saved_position) override;

    const PlanNode*
    get_plan() const override {
        return m_plan;
    }
private:

    const MergeJoin *m_plan;

    // You may add any class members here

    friend class MergeJoin;
};

}   // namespace taco

#endif  // EXECUTION_MERGEJOINSTATE_H
