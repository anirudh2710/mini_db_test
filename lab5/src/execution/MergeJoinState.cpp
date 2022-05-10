#include "execution/MergeJoinState.h"

namespace taco {

MergeJoinState::~MergeJoinState() {}

void
MergeJoinState::node_properties_to_string(std::string &buf, int indent) const {
}

void
MergeJoinState::init() {
    // TODO implement it
}

bool
MergeJoinState::next_tuple() {
    // TODO implement it
    return false;
}

std::vector<NullableDatumRef>
MergeJoinState::get_record() {
    // TODO implement it
    return {};
}

void
MergeJoinState::close() {
    // TODO implement it
}

void
MergeJoinState::rewind() {
    // TODO implement it
}

Datum
MergeJoinState::save_position() const {
    // TODO implement it
    return Datum::FromNull();
}

bool
MergeJoinState::rewind(DatumRef saved_position) {
    // TODO implement it
    return false;
}

}   // namespace taco
