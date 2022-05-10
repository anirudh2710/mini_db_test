#include "execution/IndexNestedLoopState.h"

#include "index/tuple_compare.h"

namespace taco {

IndexNestedLoopState::~IndexNestedLoopState() {

}

void
IndexNestedLoopState::node_properties_to_string(std::string &buf,
                                                int indent) const {
}

void
IndexNestedLoopState::init() {
    // TODO implement it
}

bool
IndexNestedLoopState::next_tuple() {
    // TODO implement it
    return false;
}

std::vector<NullableDatumRef>
IndexNestedLoopState::get_record() {
    return {};
}

void
IndexNestedLoopState::close() {
    // TODO implement it
}

void
IndexNestedLoopState::rewind() {
    // TODO implement it
}

Datum
IndexNestedLoopState::save_position() const {
    // TODO implement it
    return Datum::FromNull();
}

bool
IndexNestedLoopState::rewind(DatumRef saved_position) {
    // TODO implement it
    return false;
}


}   // namespace taco

