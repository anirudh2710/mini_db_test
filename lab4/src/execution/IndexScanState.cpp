#include "execution/IndexScanState.h"
#include "storage/VarlenDataPage.h"

namespace taco {

IndexScanState::IndexScanState(const IndexScan* plan,
                               std::unique_ptr<Index> idx)
: PlanExecState(TAG(IndexScan))
, m_plan(plan)
{
    // TODO: implement it
}

IndexScanState::~IndexScanState() {
    // TODO: implement it
}

void
IndexScanState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
IndexScanState::init() {
    // TODO: implement it
}

bool
IndexScanState::next_tuple() {
    // TODO: implement it
    return false;
}

std::vector<NullableDatumRef>
IndexScanState::get_record() {
    // TODO: implement it
    return {};
}

void
IndexScanState::close() {
    // TODO: implement it
}

void
IndexScanState::rewind() {
    // TODO: implement it
}

Datum
IndexScanState::save_position() const {
    // TODO: implement it
    return Datum::FromNull();
}

bool
IndexScanState::rewind(DatumRef saved_position) {
    // TODO: implement it
    return false;
}


}