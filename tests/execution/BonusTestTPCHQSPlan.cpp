#include "execution/BonusTestTPCHQS.h"

#include "catalog/aggtyp.h"
#include "plan/Selection.h"
#include "plan/CartesianProduct.h"
#include "plan/Projection.h"
#include "plan/Sort.h"
#include "plan/Limit.h"
#include "plan/Aggregation.h"
#include "plan/MergeJoin.h"
#include "plan/IndexNestedLoop.h"
#include "expr/AndOperator.h"
#include "expr/BinaryOperator.h"
#include "expr/Variable.h"
#include "expr/Literal.h"

namespace taco {

std::unique_ptr<PlanNode>
BonusTestTPCHQS::GetPlan() {
    auto param = GetParam();

    // QS (non-benchmark query): find the total difference of the value of
    // returned line itmes that may have been replaced by another line item
    // that was shipped to the same customer within a time window around the
    // receipt of the returned item.
    //
    // order 9 fields lineitem 16 fields
    // in total 50 fields after join.
    //
    // SELECT SUM(l1.l_extendedprice * (1 - l1.l_discount) -
    //            l2.l_extendedprice * (1 - l2.l_discount)), COUNT(*)
    // FROM lineitem l1, orders o1, lineitem l2, orders o2
    // WHERE l1.l_orderkey = o1.o_orderkey
    // AND l2_l_orderkey = o2.o_orderkey
    // AND l1.l_receiptdate > l2.l_shipdate
    // AND l1.l_receiptdate < l2.l_shipdate + interval [INTERVAL] day
    // AND o1.o_custkey = o2.o_custkey
    // and l1.l_returnflag = 'R'
    // and l2.l_returnflag <> 'R'
    //
    // Baseline Query Plan:
    // Aggregation_{SUM(l1.l_extendedprice * (1 - l1.l_discount) -
    //                  l2.l_extendedprice * (1 - l2.l_discount)), COUNT(1)} (
    //   Selection_{l1.l_orderkey = o1.o_orderkey
    //              AND l2_l_orderkey = o2.o_orderkey
    //              AND l1.l_receiptdate > l2.l_shipdate
    //              AND l1.l_receiptdate < l2.l_shipdate + interval [INTERVAL] day
    //              AND o1.o_custkey = o2.o_custkey
    //              AND l1.l_returnflag = 'R'
    //              AND l2.l_returnflag <> 'R'} (
    //     CartesianProduct(
    //       CartesianProduct(
    //         CartesianProduct(TableScan(lineitem), TableScan(order)),
    //         TableScan(lineitem)),
    //       TableScan(order))
    //   )
    // )
    //

    P ts_l1 = TableScan::Create(m_lineitem);
    P ts_o1 = TableScan::Create(m_orders);
    P ts_l2 = TableScan::Create(m_lineitem);
    P ts_o2 = TableScan::Create(m_orders);
    P c_lo = CartesianProduct::Create(std::move(ts_l1), std::move(ts_o1));
    P c_lol = CartesianProduct::Create(std::move(c_lo), std::move(ts_l2));
    P c_lolo = CartesianProduct::Create(std::move(c_lol), std::move(ts_o2));

    // Note: can't use GetFieldIdFromFieldName() here because we have duplicate
    // field names in the schema and we haven't implemented table aliases.
    //
    // Field ID map after joining in l1, o1, l2, o2 order:
    // l1.l_orderkey field -> 0
    // o1.o_orderkey field -> 16
    // l2.l_orderkey field -> 25
    // o2.o_orderkey field -> 41
    // l1.l_receiptdate -> 12
    // l2.l_shipdate -> 35
    // o1.o_custkey field -> 17
    // o2.o_custkey field -> 42
    // l1.l_returnflag -> 8
    // l2.l_returnflag -> 33
    // l1.l_extendedprice -> 5
    // l1.l_discount -> 6
    // l2.l_extendedprice -> 30
    // l2.l_discount -> 31

    E jp_lo1 = JoinPred(c_lolo->get_output_schema(), 0, OPTYPE(EQ), 16);
    E jp_lo2 = JoinPred(c_lolo->get_output_schema(), 25, OPTYPE(EQ), 41);
    E jp_band1 = JoinPred(c_lolo->get_output_schema(), 12, OPTYPE(GT), 35);
    std::unique_ptr<ExprNode> new_date = BinaryOperator::Create(OPTYPE(ADD),
        Variable::Create(c_lolo->get_output_schema(), 35),
        Literal::Create(Datum::From(param), initoids::TYP_INT4));
    E jp_band2 = BinaryOperator::Create(OPTYPE(LT),
        Variable::Create(c_lolo->get_output_schema(), 12), std::move(new_date));
    E jp_ooc = JoinPred(c_lolo->get_output_schema(), 17, OPTYPE(EQ), 42);
    E sc_lr1 = SelPred(c_lolo->get_output_schema(), 8, OPTYPE(EQ), "R");
    E sc_lr2 = SelPred(c_lolo->get_output_schema(), 33, OPTYPE(NE), "R");

    E s_full_cond1 = AndOperator::Create(std::move(jp_lo1), std::move(jp_lo2));
    E s_full_cond2 = AndOperator::Create(std::move(s_full_cond1), std::move(jp_band1));
    E s_full_cond3 = AndOperator::Create(std::move(s_full_cond2), std::move(jp_band2));
    E s_full_cond4 = AndOperator::Create(std::move(s_full_cond3), std::move(jp_ooc));
    E s_full_cond5 = AndOperator::Create(std::move(s_full_cond4), std::move(sc_lr1));
    E s_full_cond = AndOperator::Create(std::move(s_full_cond5), std::move(sc_lr2));

    P sel = Selection::Create(std::move(c_lolo), std::move(s_full_cond));
    std::vector<E> agg_exprs;
    E l1_revpct = BinaryOperator::Create(OPTYPE(SUB),
        Literal::Create(Datum::From((double) 1.0), initoids::TYP_DOUBLE),
        Variable::Create(sel->get_output_schema(), 6));
    E l1_rev = BinaryOperator::Create(OPTYPE(MUL),
        Variable::Create(sel->get_output_schema(), 5),
        std::move(l1_revpct));
    E l2_revpct = BinaryOperator::Create(OPTYPE(SUB),
        Literal::Create(Datum::From((double) 1.0), initoids::TYP_DOUBLE),
        Variable::Create(sel->get_output_schema(), 31));
    E l2_rev = BinaryOperator::Create(OPTYPE(MUL),
        Variable::Create(sel->get_output_schema(), 30),
        std::move(l2_revpct));
    agg_exprs.emplace_back(BinaryOperator::Create(OPTYPE(SUB),
                                                  std::move(l1_rev),
                                                  std::move(l2_rev)));

    agg_exprs.emplace_back(Literal::Create(Datum::From((uint64_t)1),
                                           initoids::TYP_UINT8));

    P agg = Aggregation::Create(std::move(sel), std::move(agg_exprs),
                                { AGGTYPE(SUM), AGGTYPE(COUNT) });

    return agg;
}

}   // namespace taco

