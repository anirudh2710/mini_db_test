#include "execution/LimitState.h"

namespace taco {

LimitState::LimitState(const Limit* plan, std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(LimitState), std::move(child)), m_plan(plan)
, m_pos(0) {}

LimitState::~LimitState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
LimitState::init() {
    ASSERT(!m_initialized);
    get_child(0)->init();
    m_initialized = true;
    m_pos = 0;
}

bool
LimitState::next_tuple() {
    if (m_pos >= m_plan->m_cnt) return false;
    if (get_child(0)->next_tuple()) {
        ++m_pos;
        return true;
    }
    return false;
}

void
LimitState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::vector<NullableDatumRef>
LimitState::get_record() {
    return get_child(0)->get_record();
}


void
LimitState::close() {
    ASSERT(m_initialized);
    m_pos = 0;
    get_child(0)->close();
    m_initialized = false;
}

void
LimitState::rewind() {
    ASSERT(m_initialized);
    get_child(0)->rewind();
    m_pos = 0;
}

Datum
LimitState::save_position() const {
    Datum d[2] = {
        get_child(0)->save_position(),
        Datum::From(m_pos)
    };
    return DataArray::From(d, 2);
}

bool
LimitState::rewind(DatumRef saved_position) {
    m_pos = DataArray::GetDatum(saved_position, 1).GetUInt8();
    Datum child_pos = DataArray::GetDatum(saved_position, 0);
    return m_pos < m_plan->m_cnt && get_child(0)->rewind(child_pos);
}


}
