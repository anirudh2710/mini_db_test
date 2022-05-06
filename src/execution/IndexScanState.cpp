#include "execution/IndexScanState.h"
#include "storage/VarlenDataPage.h"
#include "index/btree/BTreeInternal.h"

namespace taco {

IndexScanState::IndexScanState(const IndexScan* plan,
                               std::unique_ptr<Index> idx)
: PlanExecState(TAG(IndexScan))
, m_plan(plan) , m_index(std::move(idx)), m_indexval(-1)
{
    indexiterator = m_index->StartScan(m_plan-> m_indexlower, m_plan->m_index_lowerstrict, m_plan->m_indexhigher, m_plan->m_index_higherstrict);

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
    ASSERT(!m_initialized);
    //making intialized true 
    m_initialized = true;

    //making index value to be -1
    m_indexval = -1;
}

bool
IndexScanState::next_tuple() {
    //checking indexiterator 
    if (indexiterator->Next()){return true;}

    return false;
}

std::vector<NullableDatumRef>
IndexScanState::get_record() {
    std::vector<NullableDatumRef> recordval;

    recordval.clear();recordval.reserve(2);
    //to access the record value 
    std::vector<Datum> datum = m_plan->m_index->GetKeySchema()->DissemblePayload(indexiterator->GetCurrentItem().GetData());
    
    for (auto& x: datum)
    {
        recordval.emplace_back(x);
    }

    recordval.emplace_back(Datum::FromNull());
    //we return the record value thats stored 
    return recordval;
}

void
IndexScanState::close() {
    m_initialized = false;
}

void
IndexScanState::rewind() {
    //first we call the endscan func to pause
    indexiterator->EndScan();
    
    //and start the scan again
    indexiterator = m_index->StartScan(m_plan-> m_indexlower, m_plan->m_index_lowerstrict, m_plan->m_indexhigher, m_plan->m_index_higherstrict);
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