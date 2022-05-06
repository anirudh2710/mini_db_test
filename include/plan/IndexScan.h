#ifndef PLAN_INDEXSCAN_H
#define PLAN_INDEXSCAN_H

#include "plan/PlanNode.h"
#include "catalog/IndexDesc.h"
#include "catalog/TableDesc.h"
#include "index/IndexKey.h"

namespace taco {

class IndexScanState;

/*!
 * `IndexScan` is the physical plan for index based table scanning. This physical
 * plan should remember which index it is leveraging through an index descriptor
 * and the search bounds through index keys and strict indicators.
 *
 * Note that the output schema for this physical plan should be **the schema of
 * the table** associated with given index rather than simply index key + RID.
 *
 * CAUTION: You want to guarantee the memory safety of the Index Keys you passed
 * in for further use. Since those keys can be constructed in temporary scopes.
 */
class IndexScan: public PlanNode {
public:
    static std::unique_ptr<IndexScan>
    Create(std::shared_ptr<const IndexDesc> idxdesc,
           const IndexKey* low, bool lower_isstrict,
           const IndexKey* high, bool higher_isstrict);

    ~IndexScan() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    IndexScan(std::shared_ptr<const IndexDesc> idxdesc,
              const IndexKey* low, bool lower_isstrict,
              const IndexKey* high, bool higher_isstrict);
              
    const Schema* m_selectschema;
    bool m_index_lowerstrict, m_index_higherstrict;

    const IndexKey* m_indexlower;
    const IndexKey* m_indexhigher;

    std::shared_ptr<const IndexDesc> m_index;
    std::shared_ptr<const TableDesc> m_indextabledesc;

    friend class IndexScanState;
};

}    // namespace taco

#endif
