#include "execution/TableInsertState.h"
#include "storage/Table.h"

namespace taco {

TableInsertState::TableInsertState(const TableInsert* plan,
                                   std::unique_ptr<Table>&& table,
                                   std::vector<std::unique_ptr<Index>>&& idxs,
                                   std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(TableInsertState), std::move(child))
, m_plan(plan)
, m_table(std::move(table))
, m_idxs(std::move(idxs))
, m_res(-1) {}

TableInsertState::~TableInsertState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
TableInsertState::node_properties_to_string(std::string &buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
TableInsertState::init() {
    get_child(0)->init();
    ASSERT(!m_initialized);
    m_initialized = true;
    m_res = -1;
}

bool
TableInsertState::next_tuple() {
    // This function should only once!
    if (m_res >= 0) return false;
    m_res = 0;
    auto sch = m_plan->m_tabdesc->GetSchema();
    while (get_child(0)->next_tuple()) {
        maxaligned_char_buf buf;
        sch->WritePayloadToBuffer(get_child(0)->get_record(), buf);
        Record rec(buf);
        m_table->InsertRecord(rec);
        for (size_t i = 0; i < m_idxs.size(); ++i)
            m_idxs[i]->InsertRecord(rec, sch);
        ++m_res;
    }
    return true;
}

std::vector<NullableDatumRef>
TableInsertState::get_record() {
    return { Datum::From(m_res) };
}

void
TableInsertState::close() {
    ASSERT(m_initialized);
    m_initialized = false;
    get_child(0)->close();
}

void
TableInsertState::rewind() {
    LOG(kFatal, "Table actions cannot be rewinded...");
}

}
