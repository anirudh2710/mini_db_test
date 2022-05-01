#include "execution/TempTableState.h"

namespace taco {

TempTableState::TempTableState(const TempTable* plan)
: PlanExecState(TAG(TempTableState)), m_plan(plan)
, m_pos(-1) {}

TempTableState::~TempTableState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
TempTableState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
TempTableState::init() {
    ASSERT(!m_initialized);
    m_initialized = true;
    m_pos = -1;
}

bool
TempTableState::next_tuple() {
    m_pos++;
    ASSERT(m_pos >= 0);
    return ((size_t)m_pos) < m_plan->m_data.size();
}

std::vector<NullableDatumRef>
TempTableState::get_record() {
    std::vector<NullableDatumRef> res;
    res.reserve(m_plan->m_schema->GetNumFields());
    for (auto& x: m_plan->m_data[m_pos]) {
        res.emplace_back(NullableDatumRef(x));
    }
    return res;
}

void
TempTableState::close() {
    ASSERT(m_initialized);
    m_initialized = false;
}

void
TempTableState::rewind() {
    ASSERT(m_initialized);
    m_pos = -1;
}

Datum
TempTableState::save_position() const {
    return Datum::From(m_pos);
}

bool
TempTableState::rewind(DatumRef saved_position) {
    m_pos = saved_position.GetInt64();
    return m_pos >= 0 && m_pos < (int64_t) m_plan->m_data.size();
}

}
