#ifndef PLAN_AGGREGATION_H
#define PLAN_AGGREGATION_H

#include "plan/PlanNode.h"
#include "expr/ExprNode.h"
#include "catalog/systables/Aggregation.h"

namespace taco {

class AggregationState;

/*!
 * `Aggregation` is the physical plan for aggregations. This physical plan
 * should remember its child plan, aggregation types, and a set of expressions
 * conducted on input columns from its child. By the end of construction, it
 * should also look up and remember corresponding aggregation functions for
 * further execution.
 *
 * You should use `g_catcache->FindAggregationByTidAndOprType` to locate the
 * aggregation id given the type of the aggregation (see
 * include/catalog/aggtyp.h), and the type of operand (i.e., the aggregated
 * expression).  Then you can leverage
 * `g_catcache->FindAggregation(aggregation_id)` to get the construct
 * containing function IDs for initialize, accumulate and finalize function of
 * a particular aggregation operator defined by \p apptyp.
 *
 * Later on, to use these aggregate functions during execution, you can simply
 * look up function IDs through `FindBuiltinFuction()` to get the raw function
 * pointer and call the function through `FunctionCall()`.
 *
 * The output schema of aggregation should be based on the aggregation output
 * of all aggregating expressions. It can be derived from the return types of
 * aggregation expressions.
 */
class Aggregation: public PlanNode {
public:
    static std::unique_ptr<Aggregation>
    Create(std::unique_ptr<PlanNode>&& child,
           std::vector<std::unique_ptr<ExprNode>>&& exprs,
           const std::vector<AggType>& aggtyps);

    ~Aggregation() override;

    void node_properties_to_string(std::string& buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    Aggregation(std::unique_ptr<PlanNode>&& child,
                std::vector<std::unique_ptr<ExprNode>>&& exprs,
                const std::vector<AggType>& aggtyps);


    // You can add your own states here.

    std::unique_ptr<Schema> m_selectschema;

    std::vector<std::unique_ptr<ExprNode>> m_selectexpr;

    


    std::vector<Oid> m_aggstart, m_aggend, m_aggtypes, m_aggfun;

    friend class AggregationState;
};

}    // namespace taco

#endif
