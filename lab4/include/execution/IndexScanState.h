#ifndef EXECUTION_INDEXSCANSTATE_H
#define EXECUTION_INDEXSCANSTATE_H

#include "storage/BufferManager.h"

#include "execution/PlanExecState.h"
#include "plan/IndexScan.h"
#include "index/Index.h"

namespace taco {

/*!
 * `IndexScanState` is the execution state for index based table scan. This execution
 * state should return all **heap file** records statisfying the range its physical plan
 * indicates. Note that you should return heap file records rather than RecordID.
 *
 * An index access interface `Index` is passed as \p idx when constructed. All index accesses
 * to the underlying index should go through that access interface.
 *
 * CAUTION: You need to make sure the output record's `NullableDatumRef` are always memory
 * safe at any time until the operator is closed.
 */
class IndexScanState: public PlanExecState {
public:
    ~IndexScanState() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    void init() override;

    bool next_tuple() override;

    std::vector<NullableDatumRef> get_record() override;

    void close() override;

    void rewind() override;

    Datum save_position() const override;

    bool rewind(DatumRef saved_position);

    const PlanNode*
    get_plan() const {
        return m_plan;
    }

private:
    IndexScanState(const IndexScan* plan,
                   std::unique_ptr<Index> idx);

    const IndexScan* m_plan;

    // You can add your own states here.

    friend class IndexScan;
};

}    // namespace taco

#endif
