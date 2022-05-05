#include "execution/CartesianProductState.h"

namespace taco {

CartesianProductState::CartesianProductState
    (const CartesianProduct* plan,
     std::unique_ptr<PlanExecState>&& left,
     std::unique_ptr<PlanExecState>&& right)
: PlanExecState(TAG(CartesianProductState), std::move(left), std::move(right))
, m_plan(plan)
{
    m_iteration=0;
}

CartesianProductState::~CartesianProductState() {
    if (m_initialized) {
        LOG(kWarning, "not closed upon destruction");
    }
}

void
CartesianProductState::init() {
    // TODO: implement it
    get_child(0)->init();
    
    
    get_child(1)->init();


    while(get_child(0)->next_tuple())
    {
        std::vector<NullableDatumRef> left_rec = get_child(0)->get_record();

        get_child(1)->rewind();

        while(get_child(1)->next_tuple())
        {
            std::vector<NullableDatumRef> val;
            std::vector<NullableDatumRef> right_rec = get_child(1)->get_record();
            val.insert(val.end(), left_rec.begin(), left_rec.end()); 
            val.insert(val.end(), right_rec.begin(), right_rec.end()); 

            size_t count=0;
            while(count < val.size())
            {
                m_datumref.emplace_back(NullableDatumRef(val[count]));
                count++;
            }
        }
    }
}

bool
CartesianProductState::next_tuple() {
    // TODO: implement it
    m_iteration = m_iteration + m_plan->m_schemacount;
    auto ret_val = ((size_t)(m_iteration - m_plan->m_schemacount)) < m_datumref.size();
    return ret_val;
}

void
CartesianProductState::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
    
}

std::vector<NullableDatumRef>
CartesianProductState::get_record() {
    // TODO: implement it
    std::vector<NullableDatumRef> value;

    value.reserve( m_plan->m_schemacount);
    int count = (m_iteration -  m_plan->m_schemacount);

    
    while (count < m_iteration)
    {
        value.emplace_back(NullableDatumRef(m_datumref[count]));
        count++;
    }
    return value;
}

void
CartesianProductState::close() {
    // TODO: implement it
    get_child(0)->close();

    get_child(1)->close();

    m_initialized = false;
}

void
CartesianProductState::rewind() {
    // TODO: implement it
    m_iteration=0;
}

Datum
CartesianProductState::save_position() const {
    // Hint: the following allows you to pack two saved positions into one
    // single Datum. Feel free to add more to the data array if needed by your
    // implementation.
    Datum d[2] = {
        get_child(0)->save_position(),
        get_child(1)->save_position()
    };
    return DataArray::From(d, 2);
}

bool
CartesianProductState::rewind(DatumRef saved_position) {
    // Hint: the following is how you may get the saved positions from a packed
    // data array that is stored inside a datum. Feel free to rearrange lines
    // accessors in your code.
    Datum c1_spos = DataArray::GetDatum(saved_position, 0);
    Datum c2_spos = DataArray::GetDatum(saved_position, 1);

    // TODO: implement this in the next project
    return false;
}


}
