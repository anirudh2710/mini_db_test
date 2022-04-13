#ifndef EXECUTION_SELECTIONSTATE_H
#define EXECUTION_SELECTIONSTATE_H

#include "execution/PlanExecState.h"
#include "plan/Selection.h"

namespace taco {

/*!
 * `SelectionState` is the execution state for selections. This execution state
 * should always return the records that satisfy the condition for defined in
 * its physical plan.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that
 * the exeuction state pointing at is always memory safe until the operator is
 * closed.
 */
class SelectionState: public PlanExecState {
public:
    ~SelectionState() override;

    void init() override;

    bool next_tuple() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

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
    SelectionState(const Selection* plan, std::unique_ptr<PlanExecState>&& child);

    const Selection* m_plan;

    // You can add your own state here...

    friend class Selection;
};

}    // namespace taco

#endif
