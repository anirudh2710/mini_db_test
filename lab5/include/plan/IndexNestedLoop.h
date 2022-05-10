#ifndef PLAN_INDEXNESTEDLOOP_H
#define PLAN_INDEXNESTEDLOOP_H

#include "tdb.h"

#include "catalog/TableDesc.h"
#include "catalog/IndexDesc.h"
#include "catalog/Schema.h"
#include "expr/ExprNode.h"
#include "index/Index.h"
#include "plan/PlanNode.h"

namespace taco {

class IndexNestedLoopState;

/*!
 * `IndexNestedLoop` is the physical plan node for the index nested loop join
 * operator. This operator currently only handles a join between R (outer, can
 * be any plan) and S (inner, must be a relation) in the following form: E1 =
 * S.A1 and E2 = S.A2 and ... and Ek <= (or <) S.Ak and Ek' > (or >=) S.Ak,
 * where E1, E2, ... Ek are expressions over the outer relation R (which can
 * also be constant expressions for predicate push-down purposes), and A1, A2,
 * ... Ak is a prefix of the index key of the given index of the inner relation
 * S. Ek is the lower bound expression (for S.Ak) while Ek' is the upper bound
 * expression (for S.Ak).  The `<=` and `>=` are called non-strict comparison
 * operators while `<` and `>` are called the strict comparison operators. When
 * the comparison operator for the lower bound expression Ek (resp. the upper
 * bound expression Ek') is strict, we set a boolean flag `lower_isttrict`
 * (resp. `upper_isstrict`) to true in the factory method
 * `IndexNestedLoop::Create`. Otherwise, it is set to false.
 *
 * The order of the output of the index nested loop depends on the order of
 * outer plan and the ordering of keys in the inner relation index. More
 * specifically, for any two join results \f$r \circ s, r' \circ s'\f$, where
 * \f$ r, r' \in R, s, s' \in S\f$, \f$r \circ s\f$ appears before \f$r' \circ
 * s'\f$ in the output iff
 *    - r appears before r' in the output of the outer plan R
 *    - r = r' and s is ordered before s' in the index of the inner relation S
 *
 * Hints:
 *    -# All expressions in over the outer relation E1, E2, ..., Ek, Ek' should
 *    be evaluated for every record scanned from the outer relation, in order
 *    to construct the index keys for initializing the index scan.
 *    -# The last two clauses with Ek and Ek' do not need to be both present
 *       in the index nested loop join.
 *    -# The presence of Ek and/or Ek' does not directly indicate whether you
 *    should pass a lower bound key and/or an upper bound key in the
 *    index scan over the inner relation (also see below).
 *    -# When implementating the index nested loop, you might want to think
 *    about whether the implementation produces exactly the same join results
 *    as a simple nested loop (a selection over a cartesian product of two
 *    tables).  As a concrete example: suppose we have two tables R(x, y) and
 *    S(x, y) and there is a B-tree index on S(x, y). Suppose both R and S have
 *    the following four rows: (1, 1), (1, 2), (2, 1), (2, 2). Consider the
 *    following are a few different join predicates and their join result sets:
 *      - R.x < S.x: result = [(1, 1, 2, 1), (1, 1, 2, 1), (1, 2, 2, 1), (1, 2,
 *      2, 2)], k = 1, E1 = R.x, E1' = nullptr, lower_isstrict = true
 *      - R.x > S.x, result = [(2, 1, 1, 1), (2, 1, 1, 2), (2, 2, 1, 1), (2, 2,
 *      1, 2)], k = 1, E1 = nullptr, E1' = R.x, upper_isstrict = true
 *      - R.x = S.x and R.y <= S.y: result = [(1, 1, 1, 1), (1, 1, 1, 2), (1,
 *      2, 1, 2), (2, 1, 2, 1), (2, 1, 2, 2), (2, 2, 2, 2)], k = 2, E1 = R.x,
 *      E2 = R.y, E2' = nullptr, lower_isstrict = false.
 *      - R.x = S.x and R.y > S.y: result = [(1, 2, 1, 1), (2, 2, 2, 1)], k = 2,
 *      E1 = R.x, E2 = nullptr, E2' = R.y, upper_isstrict = true
 *      - R.x = S.x and R.y <= S.y and R.y >= S.y (this is how to express
 *      equi-join predicate in index nested loop): result = [(1, 1, 1, 1), (1,
 *      2, 1, 2), (2, 1, 2, 1), (2, 2, 2, 2)], k = 2, E1 = R.x, E2 = R.y, E2' =
 *      R.y, lower_isstrict = false, upper_isstrict = false.
 *    -# For the examples above, think: what are the lower and upper key
 *    that should be passed to `Index::StartScan` function to locate the
 *    correct keys? Is there any case that requires you to additionally check
 *    for any conditions in the index nested loop implementation (instead of or
 *    in addition to the upper bound key passed to `Index::StartScan`)?
 *
 * Note that we currently do not support index only scan for the inner plan.
 * The implementation must fetch the corresponding heap tuple for each data
 * entry fetched from the index.
 *
 */
class IndexNestedLoop: public PlanNode {
public:
    /*!
     * Creates an index nested loop plan node over the outer plan (which can be
     * any plan), and an inner relation with an index (note: we only have range
     * indexes right now so there is no need to check if it supports range
     * predicates). The vector \p joinexpr_outer contains the expressions E1,
     * E2, ..., Ek as described in the class documentation, and upper_expr is
     * the upper bound expression `Ek'`. A null \p upper_expr represents no
     * upper bound while a null last expression node in \p joinexprs_outer
     * represents no lower bound. It is undefined if both of them are null. All
     * the other expression nodes in the \p joinexprs_outer must not be null.
     * It is also undefined if \p joinexprs_outer is empty or has more
     * expression nodes than the number of keys in the index. The boolean
     * flags `lower_isstrict` and `upper_isstrict` indicate whether the
     * comparisons on Ek and Ek' are strict (<, >) or non-strict (<=, >=).
     *
     */
    static std::unique_ptr<IndexNestedLoop>
    Create(std::unique_ptr<PlanNode>&& outer,
           std::shared_ptr<const IndexDesc> inner_idxdesc,
           std::vector<std::unique_ptr<ExprNode>>&& joinexprs_outer,
           std::unique_ptr<ExprNode> &&upper_expr,
           bool lower_isstrict,
           bool upper_isstrict);

    ~IndexNestedLoop() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;
private:
    // You may add additional internal state here.

    friend class IndexNestedLoopState;
};

}   // namespace taco

#endif      // PLAN_INDEXNESTEDLOOP_H
