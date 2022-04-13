#include "execution/CartesianProductState.h"

namespace taco {

CartesianProductState::CartesianProductState
    (const CartesianProduct* plan,
     std::unique_ptr<PlanExecState>&& left,
     std::unique_ptr<PlanExecState>&& right)
: PlanExecState(TAG(CartesianProductState), std::move(left), std::move(right))
, m_plan(plan)
{
    // TODO: implement it
}

CartesianProductState::~CartesianProductState() {
    // TODO: implement it
}

void
CartesianProductState::init() {
    // TODO: implement it
}

bool
CartesianProductState::next_tuple() {
    // TODO: implement it
    return false;
}

void
CartesianProductState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::vector<NullableDatumRef>
CartesianProductState::get_record() {
    // TODO: implement it
    return {};
}

void
CartesianProductState::close() {
    // TODO: implement it
}

void
CartesianProductState::rewind() {
    // TODO: implement it
}

Datum
CartesianProductState::save_position() const {
    // Hint: the following allows you to pack two saved positions into one
    // single Datum. Feel free to add more to the data array if needed by your
    // implementation.
    Datum d[2] = {
        get_child(0)->save_position(),
        get_child(1)->save_position()
    };
    return DataArray::From(d, 2);
}

bool
CartesianProductState::rewind(DatumRef saved_position) {
    // Hint: the following is how you may get the saved positions from a packed
    // data array that is stored inside a datum. Feel free to rearrange lines
    // accessors in your code.
    Datum c1_spos = DataArray::GetDatum(saved_position, 0);
    Datum c2_spos = DataArray::GetDatum(saved_position, 1);

    // TODO: implement this in the next project
    return false;
}


}
