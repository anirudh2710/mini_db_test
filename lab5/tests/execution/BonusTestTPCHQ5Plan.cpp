#include "execution/BonusTestTPCHQ5.h"

#include "tdb.h"
#include "utils/builtin_funcs.h"
#include "catalog/aggtyp.h"

#include "plan/Selection.h"
#include "plan/CartesianProduct.h"
#include "plan/Projection.h"
#include "plan/Sort.h"
#include "plan/Limit.h"
#include "plan/Aggregation.h"
#include "plan/MergeJoin.h"
#include "expr/AndOperator.h"
#include "expr/BinaryOperator.h"
#include "expr/Literal.h"

namespace taco {

std::unique_ptr<PlanNode>
BonusTestTPCHQ5::GetPlan() {
    auto param = GetParam();

    // Q5 (adaptation from benchmark query Q5): Return the total revenue volume
    // generated through local suppliers within a region and a certain date
    // range.
    //
    // REGION is chosen from one of the 5 region names in TPC-H: AFRICA,
    // AMERICA, ASIA, EUROPE, MIDDLE EAST.
    // DATE is chosen from k
    //
    //
    // SELECT SUM(l_extendedprice * (1 - l_discount)), COUNT(*)
    // FROM customer, orders, lineitem, supplier, nation, region
    // WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
    // AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
    // AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
    // AND r_name = '[REGION]'
    // and o_orderdate >= date '[DATE]'
    // AND o_orderdate < date '[DATE]' + interval '365' day;
    //
    // Baseline Query Plan:
    // Aggregation_{SUM(l_extendedprice * (1 - l_discount)), COUNT(1)} (
    //   Selection_{c_custkey = o_custkey AND l_orderkey = o_orderkey
    //              AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
    //              AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
    //              AND r_name = '[REGION]' AND o_orderdate >= date '[DATE]'
    //              AND o_orderdate < date '[DATE]' + interval '365' year} (
    //     CartesianProduct(
    //       CartesianProduct(
    //         CartesianProduct(
    //           CartesianProduct(
    //             CartesianProduct(TableScan_{customer}, TableScan_{orders}),
    //             TableScan_{lineitem}),
    //           TableScan_{supplier}),
    //         TableScan_{nation}),
    //       TableScan_{region})
    //   )
    // )
    //

    P ts_c = TableScan::Create(m_customer);
    P ts_o = TableScan::Create(m_orders);
    P ts_l = TableScan::Create(m_lineitem);
    P ts_s = TableScan::Create(m_supplier);
    P ts_n = TableScan::Create(m_nation);
    P ts_r = TableScan::Create(m_region);
    P c_co = CartesianProduct::Create(std::move(ts_c), std::move(ts_o));
    P c_col = CartesianProduct::Create(std::move(c_co), std::move(ts_l));
    P c_cols = CartesianProduct::Create(std::move(c_col), std::move(ts_s));
    P c_colsn = CartesianProduct::Create(std::move(c_cols), std::move(ts_n));
    P c_colsnr = CartesianProduct::Create(std::move(c_colsn), std::move(ts_r));

    E jp_co = JoinPred(c_colsnr->get_output_schema(), "c_custkey", OPTYPE(EQ), "o_custkey");
    E jp_lo = JoinPred(c_colsnr->get_output_schema(), "l_orderkey", OPTYPE(EQ), "o_orderkey");
    E jp_ls = JoinPred(c_colsnr->get_output_schema(), "l_suppkey", OPTYPE(EQ), "s_suppkey");
    E jp_cs = JoinPred(c_colsnr->get_output_schema(), "c_nationkey", OPTYPE(EQ), "s_nationkey");
    E jp_sn = JoinPred(c_colsnr->get_output_schema(), "s_nationkey", OPTYPE(EQ), "n_nationkey");
    E jp_nr = JoinPred(c_colsnr->get_output_schema(), "n_regionkey", OPTYPE(EQ), "r_regionkey");
    E sc_r = SelPred(c_colsnr->get_output_schema(), "r_name", OPTYPE(EQ), param.first);
    E sc_o1 = SelPred(c_colsnr->get_output_schema(), "o_orderdate", OPTYPE(GE), param.second);

    auto dateinfunc = m_typid2infunc.at(initoids::TYP_DATE);

    Datum date_str = Datum::FromCString(param.second.c_str());
    Datum date = FunctionCallWithTypparam(dateinfunc, 0, date_str);
    std::unique_ptr<ExprNode> new_date = BinaryOperator::Create(OPTYPE(ADD),
                Literal::Create(date.Copy(), initoids::TYP_DATE),
                Literal::Create(Datum::From(365), initoids::TYP_INT4));
    E sc_o2 = BinaryOperator::Create(OPTYPE(LT),
                                      FieldRef(c_colsnr->get_output_schema(), "o_orderdate"),
                                      std::move(new_date));

    E s_full_cond1 = AndOperator::Create(std::move(jp_co), std::move(jp_lo));
    E s_full_cond2 = AndOperator::Create(std::move(s_full_cond1), std::move(jp_ls));
    E s_full_cond3 = AndOperator::Create(std::move(s_full_cond2), std::move(jp_cs));
    E s_full_cond4 = AndOperator::Create(std::move(s_full_cond3), std::move(jp_sn));
    E s_full_cond5 = AndOperator::Create(std::move(s_full_cond4), std::move(jp_nr));
    E s_full_cond6 = AndOperator::Create(std::move(s_full_cond5), std::move(sc_r));
    E s_full_cond7 = AndOperator::Create(std::move(s_full_cond6), std::move(sc_o1));
    E s_full_cond = AndOperator::Create(std::move(s_full_cond7), std::move(sc_o2));
    P sel = Selection::Create(std::move(c_colsnr), std::move(s_full_cond));

    std::vector<E> agg_exprs;
    E a_discount = BinaryOperator::Create(OPTYPE(SUB),
        Literal::Create(Datum::From((double)1.0), initoids::TYP_DOUBLE),
        FieldRef(sel->get_output_schema(), "l_discount"));
    agg_exprs.emplace_back(BinaryOperator::Create(OPTYPE(MUL),
        FieldRef(sel->get_output_schema(), "l_extendedprice"),
        std::move(a_discount)));

    agg_exprs.emplace_back(Literal::Create(Datum::From(1), initoids::TYP_INT4));

    P agg = Aggregation::Create(std::move(sel),
                                std::move(agg_exprs),
                                { AGGTYPE(SUM), AGGTYPE(COUNT) });

    return agg;
}

}   // namespace taco

