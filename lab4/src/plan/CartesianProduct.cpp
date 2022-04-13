#include "plan/CartesianProduct.h"
#include "execution/CartesianProductState.h"

namespace taco {

CartesianProduct::CartesianProduct(std::unique_ptr<PlanNode>&& left,
                                   std::unique_ptr<PlanNode>&& right)
: PlanNode(TAG(CartesianProduct), std::move(left), std::move(right))
{
    // TODO: implement it
}

std::unique_ptr<CartesianProduct>
CartesianProduct::Create(std::unique_ptr<PlanNode>&& left,
       std::unique_ptr<PlanNode>&& right) {

    return absl::WrapUnique(
        new CartesianProduct(std::move(left), std::move(right)));
}

CartesianProduct::~CartesianProduct() {
    // TODO: implement it
}

void
CartesianProduct::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
CartesianProduct::create_exec_state() const {
    // TODO: implement it
    return nullptr;
}

const Schema*
CartesianProduct::get_output_schema() const {
    // TODO: implement it
    return nullptr;
}

}
