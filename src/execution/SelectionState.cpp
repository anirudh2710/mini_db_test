#include "execution/SelectionState.h"

namespace taco {

SelectionState::SelectionState(const Selection* plan, std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(SelectionState), std::move(child)), m_plan(plan)
{
    // TODO: implement it
}

SelectionState::~SelectionState() {
    // TODO: implement it
}

void
SelectionState::init() {
    // TODO: implement it
}

bool
SelectionState::next_tuple() {
    // TODO: implement it
    return false;
}

void
SelectionState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::vector<NullableDatumRef>
SelectionState::get_record() {
    // TODO: implement it
    return {};
}


void
SelectionState::close() {
    // TODO: implement it
}

void
SelectionState::rewind() {
    // TODO: implement it
}

Datum
SelectionState::save_position() const {
    // TODO: implement it
    return Datum::FromNull();
}

bool
SelectionState::rewind(DatumRef saved_position) {
    // TODO: implement it
    return false;
}


}
