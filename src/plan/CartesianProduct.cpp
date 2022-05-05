#include "plan/CartesianProduct.h"
#include "execution/CartesianProductState.h"

namespace taco {

CartesianProduct::CartesianProduct(std::unique_ptr<PlanNode>&& left,
                                   std::unique_ptr<PlanNode>&& right)
: PlanNode(TAG(CartesianProduct), std::move(left), std::move(right))
{
    auto left_size = get_input_as<PlanNode>(0)->get_output_schema()->GetNumFields();
    auto right_size = get_input_as<PlanNode>(1)->get_output_schema()->GetNumFields();
    m_schemacount = left_size +  right_size;
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
    return absl::WrapUnique(new CartesianProductState(this,get_child(0)->create_exec_state(), get_child(1)->create_exec_state()));
}

const Schema*
CartesianProduct::get_output_schema() const {
    return Schema::Combine(get_input_as<PlanNode>(0)->get_output_schema(),get_input_as<PlanNode>(1)->get_output_schema());

}

}
