#ifndef PLAN_LIMIT_H
#define PLAN_LIMIT_H

#include "tdb.h"
#include "plan/PlanNode.h"

namespace taco {

class LimitState;

/*!
 * `Limit` is the physical plan for limitation. This logic plan should remember
 * its child plan and an integer of how many result record you want to truncate
 * to. The output schema should always be the same as its child.
 */
class Limit: public PlanNode {
public:
    static std::unique_ptr<Limit>
    Create(std::unique_ptr<PlanNode>&& child, size_t count);

    ~Limit() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    Limit(std::unique_ptr<PlanNode>&& child, size_t count);

    size_t m_cnt;

    friend class LimitState;
};

}    // namespace taco

#endif
