#ifndef PLAN_MERGEJOIN_H
#define PLAN_MERGEJOIN_H

#include "tdb.h"

#include "expr/ExprNode.h"
#include "plan/PlanNode.h"

namespace taco {

class MergeJoinState;

/*!
 * `MergeJoin` is the physical plan node for the merge join operator. This merge
 * join operator only handles equi-joins of R (outer) and S (inner) with
 * conjunctions of simple predicates, i.e., E1 = E1' and E2 = E2' and ... Ek =
 * Ek', where E1, E2, ..., Ek are expressions over R and E1', E2', ... Ek' are
 * expressions over S. Merge join operator assumes both the outer (resp. inner)
 * relation is sorted by the join expressions (E1, E2, ..., Ek) (resp. (E1',
 * E2', ..., Ek')), which is usually satisfied by explicitly placing a sort
 * operator or via an index scan over a tree index over the expressions. It
 * also assumes the underlying operators support `save_position()` and
 * `rewind(DatumRef)` in their corresponding `PlanExecState`.
 */
class MergeJoin: public PlanNode {
public:
    /*!
     * Creates a merge-join for an equi-join between the `outer` and `inner`
     * plan with the join expressions specified in `joinexprs_outer` and
     * `joinexprs_inner`.
     *
     * It is undefined if `joinexprs_outer` and `joinexprs_inner` do not match
     * in size, or either of `outer` or `inner` does not satisfy the
     * requirements as given in the class documentation.
     */
    static std::unique_ptr<MergeJoin>
    Create(std::unique_ptr<PlanNode>&& outer,
           std::unique_ptr<PlanNode>&& inner,
           std::vector<std::unique_ptr<ExprNode>>&& joinexprs_outer,
           std::vector<std::unique_ptr<ExprNode>>&& joinexprs_inner);

    ~MergeJoin() override;

    void node_properties_to_string(std::string &buf, int indent) const override;

    std::unique_ptr<PlanExecState> create_exec_state() const override;

    const Schema* get_output_schema() const override;

private:
    // TODO you may add any class members here

    friend class MergeJoinState;
};

}   // namespace taco

#endif      // PLAN_MERGEJOIN_H
