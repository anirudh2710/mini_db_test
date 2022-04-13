#ifndef PLAN_CARTESIANPRODUCT_H
#define PLAN_CARTESIANPRODUCT_H

#include "plan/PlanNode.h"
#include "catalog/Schema.h"

namespace taco {

class CartesianProductState;

/*!
 * `CartesianProduct` is the physical plan for Cartesian products. This physical
 * plan should remember its child plans (left and right).
 */
class CartesianProduct: public PlanNode {
public:
    static std::unique_ptr<CartesianProduct>
    Create(std::unique_ptr<PlanNode>&& left,
           std::unique_ptr<PlanNode>&& right);

    ~CartesianProduct() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    CartesianProduct(std::unique_ptr<PlanNode>&& left,
                     std::unique_ptr<PlanNode>&& right);

    // You can add your own states here.

    friend class CartesianProductState;
};

}    // namespace taco

#endif
