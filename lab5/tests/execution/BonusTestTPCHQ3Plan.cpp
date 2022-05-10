#include "execution/BonusTestTPCHQ3.h"

#include "plan/Selection.h"
#include "plan/CartesianProduct.h"
#include "plan/Projection.h"
#include "plan/Sort.h"
#include "plan/Limit.h"
#include "expr/AndOperator.h"

namespace taco {

std::unique_ptr<PlanNode>
BonusTestTPCHQ3::GetPlan() {
    // parameter: SEGMENT, DATE
    auto param = GetParam();

    // Q3 (adaptation from benchmark query Q3): find top 100 unfulfilled orders
    // ealier than [DATE] for [SEGMENT]'s customers with highest ship priority,
    // highest total price and ealiest date.
    //
    // [SEGMENT] will be replaced with the test parameter as a literal
    // [DATE] will be replaced with the test parameter as a literal
    //
    // SELECT o_orderkey, o_orderdate, o_totalprice, o_shippriority
    // FROM customer, orders
    // WHERE c_mktsegment = '[SEGMENT]'
    // AND c_custkey = o_custkey
    // AND o_orderdate < date '[DATE]'
    // AND o_orderstatus <> 'F'
    // ORDER BY o_shippriority DESC, o_totalprice DESC, o_orderdate ASC
    // LIMIT 100;
    //
    // Baseline Query Plan:
    // Limit_{100}(
    //   Sort_{o_shippriority DESC, o_totalprice DESC, o_orderdate ASC} (
    //     Projection_{o_orderkey, o_orderdate, o_totalprice, o_shippriority} (
    //       Selection_{c_mktsegment = '[SEGMENT]' AND c_custkey = o_custkey
    //                  AND o_orderdate < date '[DATE]' AND o_orderstatus <> 'F'} (
    //         CartesianProduct_{TableScan_{customer}, TableScan_{orders}}
    //       )
    //     )
    //   )
    // )


    // TODO replace the following reference implementation with a more
    // efficient plan

    P ts_c = TableScan::Create(m_customer);
    P ts_o = TableScan::Create(m_orders);
    P c_co = CartesianProduct::Create(std::move(ts_c), std::move(ts_o));
    E s_c_pred = SelPred(c_co->get_output_schema(), "c_mktsegment",
                         OPTYPE(EQ), param.first);
    E s_join_pred = JoinPred(c_co->get_output_schema(), "c_custkey",
                             OPTYPE(EQ), "o_custkey");
    E s_o_pred_1 = SelPred(c_co->get_output_schema(), "o_orderstatus",
                           OPTYPE(NE), "F");
    E s_o_pred_2 = SelPred(c_co->get_output_schema(), "o_orderdate",
                          OPTYPE(LT), param.second);
    E s_co_pred =
        AndOperator::Create(
            AndOperator::Create(
                AndOperator::Create(std::move(s_c_pred),
                                    std::move(s_join_pred)),
            std::move(s_o_pred_1)),
        std::move(s_o_pred_2));
    P s_co = Selection::Create(std::move(c_co), std::move(s_co_pred));
    std::vector<E> p_co_list;
    p_co_list.emplace_back(FieldRef(s_co->get_output_schema(), "o_orderkey"));
    p_co_list.emplace_back(FieldRef(s_co->get_output_schema(), "o_orderdate"));
    p_co_list.emplace_back(FieldRef(s_co->get_output_schema(), "o_totalprice"));
    p_co_list.emplace_back(FieldRef(s_co->get_output_schema(), "o_shippriority"));
    P p_co = Projection::Create(std::move(s_co), std::move(p_co_list));
    std::vector<E> sort_co_list;
    sort_co_list.emplace_back(Variable::Create(p_co->get_output_schema(), 3));
    sort_co_list.emplace_back(Variable::Create(p_co->get_output_schema(), 2));
    sort_co_list.emplace_back(Variable::Create(p_co->get_output_schema(), 1));
    P sort_nc = Sort::Create(std::move(p_co), std::move(sort_co_list),
                             { true, true, false });
    P limit_nc = Limit::Create(std::move(sort_nc), 100);

    return limit_nc;
}

}   // namespace taco

