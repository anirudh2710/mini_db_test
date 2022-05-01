#include "execution/ProjectionState.h"

namespace taco {

ProjectionState::ProjectionState(const Projection* plan, std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(ProjectionState), std::move(child)), m_plan(plan)
{
    m_fields.reserve(plan->m_exprs.size());
    for (size_t i = 0; i < plan->m_exprs.size(); ++i) {
        m_fields.emplace_back(Datum::FromNull());
    }
}

ProjectionState::~ProjectionState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
ProjectionState::init() {
    ASSERT(!m_initialized);
    get_child(0)->init();
    m_initialized = true;
}

bool
ProjectionState::next_tuple() {
    if (get_child(0)->next_tuple()) {
        m_fields.clear();
        std::vector<NullableDatumRef> child_output = get_child(0)->get_record();
        for (size_t i = 0; i < m_plan->m_exprs.size(); ++i) {
            m_fields.emplace_back(m_plan->m_exprs[i]->Eval(child_output));
        }
        return true;
    }
    return false;
}

void
ProjectionState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::vector<NullableDatumRef>
ProjectionState::get_record() {
    std::vector<NullableDatumRef> res;
    res.reserve(m_fields.size());
    for (size_t fid = 0; fid < m_fields.size(); ++fid) {
        res.emplace_back(NullableDatumRef(m_fields[fid]));
    }

    return res;
}

void
ProjectionState::close() {
    ASSERT(m_initialized);
    get_child(0)->close();
    m_initialized = false;
}

void
ProjectionState::rewind() {
    ASSERT(m_initialized);
    get_child(0)->rewind();
}

Datum
ProjectionState::save_position() const {
    return get_child(0)->save_position();
}

bool
ProjectionState::rewind(DatumRef saved_position) {
    PlanExecState *c = get_child(0);
    if (c->rewind(saved_position)) {
        // need to recompute the projection list here
        m_fields.clear();
        std::vector<NullableDatumRef> child_output = c->get_record();
        for (size_t i = 0; i < m_plan->m_exprs.size(); ++i) {
            m_plan->m_exprs[i]->Eval(child_output);
        }
        return true;
    }

    return false;
}

}
