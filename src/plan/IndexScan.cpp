#include "plan/IndexScan.h"
#include "execution/IndexScanState.h"
#include "catalog/CatCache.h"

namespace taco {

IndexScan::IndexScan(std::shared_ptr<const IndexDesc> idxdesc,
                     const IndexKey* low, bool lower_isstrict,
                     const IndexKey* high, bool higher_isstrict)
: PlanNode(TAG(IndexScan))
{
    // TODO: implement it
    // Hint: you might want to copy and then deepcopy the index keys here.  In
    // addition, to deepcopy two index keys using the same data buffer, make
    // sure to reserve the space before calling IndexKey::DeepCopy().  The
    // DeepCopy function could result in dangling pointer if the vector is ever
    // resized! (See IndexKey.h for details).
}

std::unique_ptr<IndexScan>
IndexScan::Create(std::shared_ptr<const IndexDesc> idxdesc,
                  const IndexKey* low, bool lower_isstrict,
                  const IndexKey* high, bool higher_isstrict) {
    return absl::WrapUnique(
        new IndexScan(idxdesc, low, lower_isstrict, high, higher_isstrict));
}

IndexScan::~IndexScan() {
    // TODO: implement it
}

void
IndexScan::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
IndexScan::create_exec_state() const {
    // TODO: implement it
    return nullptr;
}

const Schema*
IndexScan::get_output_schema() const {
    // TODO: implement it
    return nullptr;
}

};
