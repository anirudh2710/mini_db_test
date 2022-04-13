#ifndef EXECUTION_PROJECTIONSTATE_H
#define EXECUTION_PROJECTIONSTATE_H

#include "execution/PlanExecState.h"
#include "plan/Projection.h"

namespace taco {

/*!
 * `ProjectionState` is the execution state for projections. This execution
 * state should always return the record with new schema and all individual
 * expressions evaluated.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that
 * the exeuction state pointing at is always memory safe until the operator is
 * closed.
 */
class ProjectionState: public PlanExecState {
public:
    ~ProjectionState() override;

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
    ProjectionState(const Projection* plan, std::unique_ptr<PlanExecState>&& child);

    const Projection*       m_plan;
    std::vector<Datum>      m_fields;

    friend class Projection;
};

}    // namespace taco

#endif
