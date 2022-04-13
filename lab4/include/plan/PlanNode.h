#ifndef PLAN_PLANNODE_H
#define PLAN_PLANNODE_H

#include "tdb.h"

#include <memory>
#include <vector>

#include "catalog/Schema.h"
#include "utils/tree/TreeNode.h"
#include "catalog/systables/initoids.h"

namespace taco {

class PlanExecState;

/*!
 * `PlanNode` is an abstract interface representing physical query plan that
 * does not bound with any particular execution state. More than one
 * `PlanExecState` can be generated from the same physical plan, allowing
 * multiple users executing the same plan at different stage concurrently. An
 * instance of `PlanNode` should contain **all information related to the plan
 * itself** (e.g. selection condition, projection attributes, etc) but not how
 * far the users have retrieve its output record.
 */
class PlanNode: public TreeNode {
public:
    /*!
     * Base constructor for all physical plan node. It is marked with a node
     * tag (see include/utils/tree/node_Tags.inc for more details) and unique
     * pointers to all of its children execution states. The unique pointers
     * to its children will be moved to `TreeNode` internal states after the
     * construction so you don't have to remember it separately.
     *
     * Note: You can use `TAG(ClassName)` to get the corresponding tag for a
     * plan execution state. For example, `TAG(Selection)` will give you the
     * tag of `Selection`.
     */
    template<class ...UniquePtrs>
    PlanNode(NodeTag tag, UniquePtrs&& ...input):
        TreeNode(tag, std::forward<UniquePtrs>(input)...) {}

    //! Deconstructor of PlanExecState.
    virtual ~PlanNode() {}

    /*!
     * Create the corresponding execution state for the physical plan so that
     * it can be used as physical scan operator later. This should create an
     * execution state matching the current physical plan, all its descendant
     * execution states, and link them together.
     */
    virtual std::unique_ptr<PlanExecState> create_exec_state() const = 0;

    /*!
     * Each physical plan should link a schema for its output relation,
     * incidating what fields are included in its output records. This
     * function returns the output schema of this physical plan.
     *
     * NOTE: if you want to create a schema owned by the plan, you should
     * make sure `ComputeLayout()` is called upon the newly created plan
     * so that it can be used in serialize/deserialize records.
     */
    virtual const Schema* get_output_schema() const = 0;

protected:
    /*!
     * Utility function to get the raw pointer of \p i-th child of this
     * physical plan.
     */
    PlanNode* get_child(size_t i) const {
        return get_input_as<PlanNode>(i);
    }
};

}   // namespace taco

#endif
