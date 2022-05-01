#include "execution/SortState.h"

#include <absl/flags/flag.h>

ABSL_FLAG(size_t, sort_nways, 8,
          "performs n-way external sorting in the sort operator");

namespace taco {

SortState::SortState(const Sort* plan,
                     std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(SortState), std::move(child))
, m_plan(plan)
{
    // TODO: implement it
}

SortState::~SortState() {
    // TODO: implement it
}

void
SortState::init() {
    // TODO: implement it
}

bool
SortState::next_tuple() {
    // TODO: implement it
    return false;
}

void
SortState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::vector<NullableDatumRef>
SortState::get_record() {
    // TODO: implement it
    return {};
}

void
SortState::close() {
    // TODO: implement it
}

void
SortState::rewind() {
    // TODO: implement it
}

Datum
SortState::save_position() const {
    // TODO: implement it
    return Datum::FromNull();
}

bool
SortState::rewind(DatumRef saved_position) {
    // TODO: implement it
    return false;
}

}
