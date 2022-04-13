#ifndef EXECUTION_CARTESIANPRODUCTSTATE_H
#define EXECUTION_CARTESIANPRODUCTSTATE_H

#include "execution/PlanExecState.h"
#include "plan/CartesianProduct.h"

namespace taco {

/*!
 * `CartesianProductState` is the execution state for cartesian product. This
 * execution state should iterate through all `n * m` output records once.
 *
 * CAUTION: You need to make sure the current record's `NullableDatumRef` that the exeuction
 * state pointing at is always memory safe until the operator is closed
 */
class CartesianProductState: public PlanExecState {
public:
    ~CartesianProductState() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

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
    CartesianProductState(const CartesianProduct *plan,
                          std::unique_ptr<PlanExecState>&& left,
                          std::unique_ptr<PlanExecState>&& right);

    const CartesianProduct* m_plan;

    // You can add your own states here.

    friend class CartesianProduct;
};

}    // namespace taco

#endif
