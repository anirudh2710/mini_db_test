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

    // TODO you may add any class members here

    friend class IndexScanState;
};

}    // namespace taco

#endif
