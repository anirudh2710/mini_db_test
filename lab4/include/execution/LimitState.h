#ifndef EXECUTION_LIMITSTATE_H
#define EXECUTION_LIMITSTATE_H

#include "execution/PlanExecState.h"
#include "plan/Limit.h"

namespace taco {

/*!
 * `LimitState` is the exeuction state for limitation. This execution state
 * should always return the first `n` records yielded out by its child
 * deterministicly. The number of `n` is defined in its physical plan.
 * If the child plan yields less than `n` records, this execution plan yields
 * as much as the child goes.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that
 * the exeuction state pointing at is always memory safe until the operator is
 * closed.
 */
class LimitState: public PlanExecState {
public:
    ~LimitState() override;

    void init() override;

    bool next_tuple() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

    std::vector<NullableDatumRef> get_record() override;

    void close() override;

    void rewind() override;

    const PlanNode*
    get_plan() const override {
        return m_plan;
    }

    Datum save_position() const override;

    bool rewind(DatumRef saved_position) override;

private:
    LimitState(const Limit* plan, std::unique_ptr<PlanExecState>&& child);

    const Limit* m_plan;

    size_t m_pos;

    friend class Limit;
};

}    // namespace taco

#endif
