#ifndef EXECUTION_INDEXNESTEDLOOPSTATE_H
#define EXECUTION_INDEXNESTEDLOOPSTATE_H

#include "tdb.h"

#include "storage/Table.h"
#include "index/Index.h"
#include "index/IndexKey.h"
#include "plan/IndexNestedLoop.h"
#include "execution/PlanExecState.h"

namespace taco {

/*!
 * `IndexNestedLoopState` is the execution state for merge joins. This
 * execution state should be able to iterate through all join results.
 * You don't need to guarantee any order for iteration.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that
 * the exeuction state pointing at is always memory safe until the operator is
 * closed.
 */
class IndexNestedLoopState: public PlanExecState {
public:
    ~IndexNestedLoopState() override;

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

    const IndexNestedLoop *m_plan;
    // TODO you may add any class members here.

    friend class IndexNestedLoop;
};

}   // namespace taco


#endif      // EXECUTION_INDEXNESTEDLOOPSTATE_H
