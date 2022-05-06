#ifndef EXECUTION_AGGREGATION_H
#define EXECUTION_AGGREGATION_H

#include "execution/PlanExecState.h"
#include "plan/Aggregation.h"

namespace taco {

/*!
 * `AggregationState` is the execution state for aggregation. This execution
 * state should always return one and only one record which is the aggregation
 * result.
 *
 * CAUTION: You need to make sure the aggregation result Datum is memory safe
 * until the operator is closed.
 */
class AggregationState: public PlanExecState {
public:
    ~AggregationState() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

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
    AggregationState(const Aggregation* scan,
                     std::unique_ptr<PlanExecState>&& child);

    const Aggregation* m_plan;
    std::vector<Datum> m_datum;
    bool m_check = false;
    

    friend class Aggregation;
};

}    // namespace taco

#endif
