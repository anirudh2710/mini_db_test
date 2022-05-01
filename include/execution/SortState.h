#ifndef EXECUTION_SORTSTATE_H
#define EXECUTION_SORTSTATE_H

#include "execution/PlanExecState.h"
#include "plan/Sort.h"
#include "extsort/ExternalSort.h"

namespace taco {

/*!
 * `SortState` is the execution state for sorting. This execution state
 * should always iterate through all records from its subplan in a user-defined
 * order defined in its physical plan.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that
 * the exeuction state pointing at is always memory safe until the operator is
 * closed.
 */
class SortState: public PlanExecState {
public:
    ~SortState() override;

    void init() override;

    bool next_tuple() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

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
    SortState(const Sort* plan, std::unique_ptr<PlanExecState>&& child);

    const Sort* m_plan;

    // You can add your own states here.

    friend class Sort;
};

}    // namespace taco

#endif
