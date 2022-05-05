#include "execution/SelectionState.h"

namespace taco {

SelectionState::SelectionState(const Selection* plan, std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(SelectionState), std::move(child)), m_plan(plan)
{
    // TODO: implement it
}

SelectionState::~SelectionState() {
    if (m_initialized)
    
    {
        LOG(kWarning, "not closed ");
    }
}

void
SelectionState::init() {
    // TODO: implement it

    get_child(0)->init();
    m_initialized = true;
}

bool
SelectionState::next_tuple() {
    
    if (get_child(0)->next_tuple()) 
    
    {
        if(m_plan->m_selectexpr->Eval( get_child(0)->get_record()).GetBool())
        {
            return true;
        }
        else 
        {
            return SelectionState::next_tuple();
        }
    }
    return false;
}

void
SelectionState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::vector<NullableDatumRef>
SelectionState::get_record() {
    // TODO: implement it
    return get_child(0)->get_record();
}


void
SelectionState::close() {
    // TODO: implement it
    m_initialized = false;
}

void
SelectionState::rewind() {
    // TODO: implement it
    get_child(0)->rewind();
}

Datum
SelectionState::save_position() const {
    // TODO: implement it
    return get_child(0)->save_position();
}

bool
SelectionState::rewind(DatumRef saved_position) {
    
    if (get_child(0)->rewind(saved_position))
    {
        m_plan->m_selectexpr->Eval(get_child(0)->get_record()).GetBool();
        return true;
    }
    
    return false;
}


}