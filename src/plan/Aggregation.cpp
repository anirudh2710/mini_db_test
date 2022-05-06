using namespace std;
#include "catalog/CatCache.h"
#include "plan/Aggregation.h"
#include "execution/AggregationState.h"

namespace taco {

Aggregation::Aggregation(std::unique_ptr<PlanNode>&& child,
                         std::vector<std::unique_ptr<ExprNode>>&& exprs,
                         const std::vector<AggType>& aggtyps)
: PlanNode(TAG(Aggregation), std::move(child))
,  m_selectexpr(std::move(exprs))
{
    // TODO: implement it
    size_t count=0;
    while(count < m_selectexpr.size())
    {
        Oid aggid = g_catcache->FindAggregationByTidAndOprType(aggtyps.at(count), m_selectexpr[count]->ReturnType()->typid());

        shared_ptr<const SysTable_Aggregation> agg_systable = g_catcache->FindAggregation(aggid);
        
        m_aggtypes.emplace_back(agg_systable->aggrettypid());

        auto initfuncid = agg_systable->agginitfuncid();
        m_aggstart.emplace_back(move(initfuncid));

        auto accfuncid = agg_systable->aggaccfuncid();
        m_aggfun.emplace_back(move(accfuncid));
        
        auto finalizefuncid = agg_systable->aggfinalizefuncid();
        m_aggend.emplace_back(move(finalizefuncid));
        ++count;
    }

    vector<bool> nullables(m_selectexpr.size(), true);

    vector<uint64_t> typeParams(m_selectexpr.size(), 0);

    m_selectschema= absl::WrapUnique(Schema::Create(m_aggtypes, typeParams, nullables));

    m_selectschema->ComputeLayout();
}

Aggregation::~Aggregation() {
    // TODO: implement it
}

std::unique_ptr<Aggregation>
Aggregation::Create(std::unique_ptr<PlanNode>&& child,
                    std::vector<std::unique_ptr<ExprNode>>&& exprs,
                    const std::vector<AggType>& aggtyp) {
    return absl::WrapUnique(
        new Aggregation(std::move(child), std::move(exprs), aggtyp));
}

void
Aggregation::node_properties_to_string(std::string& buf, int indent) const {
    // For debugging purpose, you can implement it base on your need.
}

std::unique_ptr<PlanExecState>
Aggregation::create_exec_state() const {
    // TODO: implement it
    
    return absl::WrapUnique(new AggregationState(this, std::move(get_input_as<PlanNode>(0)->create_exec_state())));
}

const Schema*
Aggregation::get_output_schema() const {
    // TODO: implement it
    return m_selectschema.get();
}

};
