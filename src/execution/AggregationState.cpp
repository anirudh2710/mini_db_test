using namespace std;
#include "execution/AggregationState.h"
#include "utils/builtin_funcs.h"

namespace taco {

AggregationState::AggregationState(
    const Aggregation* plan,
    std::unique_ptr<PlanExecState>&& child)
: PlanExecState(TAG(AggregationState), std::move(child))
, m_plan(plan)
{
    // TODO: implement it
}

AggregationState::~AggregationState() {
    // TODO: implement it
}

void
AggregationState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

void
AggregationState::init() {

    get_child(0)->init();

    m_check = false;

    m_initialized = true;
}

bool
AggregationState::next_tuple() {
    if(!m_check){

        m_datum.clear();
        size_t count = 0;

        while(count < m_plan->m_aggfun.size())
        {
            m_datum.emplace_back(FunctionCall(FindBuiltinFunction(m_plan->m_aggstart[count])));
            ++count;
        }

        while (get_child(0)->next_tuple()) {
            vector<NullableDatumRef> child = get_child(0)->get_record();
            size_t count_1 = 0;
            while (count_1< m_plan->m_aggfun.size()) 
            {
                FunctionCall( FindBuiltinFunction(m_plan->m_aggfun[count_1]), m_datum[count_1], m_plan->m_selectexpr[count_1]->Eval(child));

                ++count_1;
            }
        } 
        size_t count_2 = 0;
        while(count_2 < m_plan->m_aggfun.size())
        {
            m_datum[count_2] = FunctionCall(FindBuiltinFunction(m_plan->m_aggend[count_2]), m_datum[count_2]);

            ++count_2;
        }
        m_check = true;

        return true;
    }
    return false;
}

std::vector<NullableDatumRef>
AggregationState::get_record() {
    // TODO: implement it
    size_t funcid = 0;
    std::vector<NullableDatumRef> datum;


    datum.reserve(m_datum.size());
    
    while(funcid < m_datum.size()) 
    {
        datum.emplace_back(NullableDatumRef(m_datum[funcid]));

        ++funcid;
    }
    return datum;
}

void
AggregationState::close() {
    // TODO: implement it
    get_child(0)->close();

    m_initialized = false;

    m_check = false;
}

void
AggregationState::rewind() {
    // TODO: implement it
    get_child(0)->rewind();

    m_check = false;
}

Datum
AggregationState::save_position() const {
    // TODO: implement it
    return Datum::FromNull();
}

bool
AggregationState::rewind(DatumRef saved_position) {
    // TODO: implement it
    return false;
}



}
