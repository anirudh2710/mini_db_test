#include "execution/AggregationState.h"
#include "utils/builtin_funcs.h"

namespace taco {

AggregationState::AggregationState(
    const Aggregation* plan,
    std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(AggregationState), std::move(child))
, m_plan(plan)
{
    // TODO: implement it
}

AggregationState::~AggregationState() {
    // TODO: implement it
}

void
AggregationState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
AggregationState::init() {
    // TODO: implement it
}

bool
AggregationState::next_tuple() {
    // TODO: implement it
    return false;
}

std::vector<NullableDatumRef>
AggregationState::get_record() {
    // TODO: implement it
    return {};
}

void
AggregationState::close() {
    // TODO: implement it
}

void
AggregationState::rewind() {
    // TODO: implement it
}

Datum
AggregationState::save_position() const {
    // TODO: implement it
    return Datum::FromNull();
}

bool
AggregationState::rewind(DatumRef saved_position) {
    // TODO: implement it
    return false;
}



}
