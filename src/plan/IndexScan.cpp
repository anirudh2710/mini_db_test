#include "plan/IndexScan.h"
#include "execution/IndexScanState.h"
#include "catalog/CatCache.h"

namespace taco {

IndexScan::IndexScan(std::shared_ptr<const IndexDesc> idxdesc,
                     const IndexKey* low, bool lower_isstrict,
                     const IndexKey* high, bool higher_isstrict)
: PlanNode(TAG(IndexScan))
, m_index(std::move(idxdesc))
{
    // TODO: implement it
    // Hint: you might want to copy and then deepcopy the index keys here.  In
    // addition, to deepcopy two index keys using the same data buffer, make
    // sure to reserve the space before calling IndexKey::DeepCopy().  The
    // DeepCopy function could result in dangling pointer if the vector is ever
    // resized! (See IndexKey.h for details).
    Oid table_id = m_index->GetIndexEntry()->idxtabid();
    std::shared_ptr<const TableDesc> table_descriptor = g_catcache->FindTableDesc(table_id);
    
    //passing the table descriptor
    m_selectschema = table_descriptor->GetSchema();

    m_indextabledesc = table_descriptor;

    //passing the low and high indexkeys
    m_indexlower = low;
    m_indexhigher = high;

    //passing the values to the declared variables of is strict
    m_index_lowerstrict = lower_isstrict;
    m_index_higherstrict = higher_isstrict;
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
    //new IndexScan(idxdesc, low, lower_isstrict, high, higher_isstrict)

}

void
IndexScan::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
IndexScan::create_exec_state() const {
    //declaring an indexpointer
    std::unique_ptr<Index> indexptr;

    //declaring two auto varibales for table creation and finding indexes
    //auto table = Table::Create(m_indextabledesc);
    //auto indexptr_oid = g_catcache->FindAllIndexesOfTable(m_indextabledesc->GetTableEntry()->tabid());
    
    for (auto x: g_catcache->FindAllIndexesOfTable(m_indextabledesc->GetTableEntry()->tabid())) 
    {
        indexptr= Index::Create(g_catcache->FindIndexDesc(x));
    }

    return absl::WrapUnique(new IndexScanState(this, std::move(indexptr)));
}

const Schema*
IndexScan::get_output_schema() const {
    // TODO: implement it
    return m_selectschema;
}

};
