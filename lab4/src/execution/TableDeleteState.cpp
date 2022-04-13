#include "execution/TableDeleteState.h"
#include "storage/Table.h"
#include "storage/VarlenDataPage.h"

namespace taco {

TableDeleteState::TableDeleteState(const TableDelete* plan,
                                   std::unique_ptr<Table>&& table,
                                   std::vector<std::unique_ptr<Index>>&& idxs)
: PlanExecState(TAG(TableDeleteState))
, m_plan(plan)
, m_table(std::move(table))
, m_idxs(std::move(idxs))
, m_res(-1) {}

TableDeleteState::~TableDeleteState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
TableDeleteState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
TableDeleteState::init() {
    ASSERT(!m_initialized);
    m_initialized = true;
    m_res = -1;
}

bool
TableDeleteState::next_tuple() {
    if (m_res >= 0) return false;
    m_res = 0;
    auto it = m_table->StartScan();
    auto sch = m_plan->m_tabdesc->GetSchema();
    std::vector<RecordId> rec_ids;
    while (it.Next()) {
        auto payload = sch->DissemblePayload(it.GetCurrentRecord().GetData());
        std::vector<NullableDatumRef> record;
        for (auto& x: payload) record.emplace_back(x);
        if (m_plan->m_cond->Eval(record).GetBool()) {
            rec_ids.emplace_back(it.GetCurrentRecordId());
        }
    }

    for (auto recid: rec_ids) {
        char* buf;
        ScopedBufferId bufid = g_bufman->PinPage(recid.pid, &buf);
        VarlenDataPage dp(buf);
        auto rec = dp.GetRecord(recid.sid);
        rec.GetRecordId().pid = recid.pid;

        for (size_t i = 0; i < m_idxs.size(); ++i)
            m_idxs[i]->DeleteRecord(rec, sch);
        m_table->EraseRecord(recid);
        m_res++;
    }
    return true;
}

std::vector<NullableDatumRef>
TableDeleteState::get_record() {
    return { Datum::From(m_res) };
}

void
TableDeleteState::close() {
    ASSERT(m_initialized);
    m_initialized = false;
}

void
TableDeleteState::rewind() {
    LOG(kFatal, "Table actions cannot be rewinded...");
}

}
