#ifndef EXECUTION_PLANEXECSTATE_H
#define EXECUTION_PLANEXECSTATE_H

#include "tdb.h"

#include "storage/Record.h"
#include "utils/tree/TreeNode.h"

namespace taco {

class PlanNode;

/*!
 * `PlanExecState` is an abstract interface for execution state of various
 * query plan. Each execution state should correspond to a query plan. It
 * operates like as an iterator iterating through the records produced by
 * a plan for a specific query instance.
 */
class PlanExecState: public TreeNode {
public:
    /*!
     * Base constructor for all execution states. It is marked with a node
     * tag (see include/utils/tree/node_Tags.inc for more details) and unique
     * pointers to all of its children execution states. When just constructed,
     * the execution states are NOT initialized until `init()` is called.
     *
     * The unique pointers to its children will be moved to `TreeNode` internal
     * states after the construction so you don't have to remember it
     * separately.
     *
     * Note: You can use `TAG(ClassName)` to get the corresponding tag for a
     * plan execution state. For example, `TAG(SelectionState)` will give you
     * the tag of `SelectionState`.
     */
    template<class ...UniquePtrs>
    PlanExecState(NodeTag tag, UniquePtrs&& ...input):
        TreeNode(tag, std::forward<UniquePtrs>(input)...),
        m_initialized(false) {}

    /*!
     * Deconstructor of PlanExecState. When it is destroyed, you have to make
     * sure this execution state is no longer initialized. Otherwise, its
     * behavior is undefined.
     */
    virtual ~PlanExecState() {}

    /*!
     * Initialize this plan execution state, set `m_initialized` to true and
     * initialize all corresponding states (including its children) to get ready
     * for query processing.
     */
    virtual void init() = 0;

    /*!
     * Moves the iterator of this execution state to the next output record.
     * Returns `true` if there is such a record. The caller should be able to
     * get the current **deserialized** record through `get_record()` before
     * another `next_tuple()` is called or the plan execution state is
     * implicitly or explicitly ended. You should make sure memory holding this
     * record is safe (on pinned page or copied somewhere) until the next
     * `next_tuple()` call.
     */
    virtual bool next_tuple() = 0;

    /*!
     * Return the deserialized output record to which this execution state
     * currently points. The deserialized record represented by a vector of
     * `NullableDatumRef` should be SAFE to access until the `next_tuple()` is
     * called again on the same execution state.
     *
     * It should return the first output record **after** the first
     * **next_tuple** call. The behavior of calling `get_record()` before the
     * first `next_tuple()` is undefined.
     */
    virtual std::vector<NullableDatumRef> get_record() = 0;

    /*!
     * Clear internal states, mark we have finish retrieving output records
     * from this execution state, and mark the execution state no longer
     * initialzied.
     *
     * A closed operator may be reopened by calling `init()` again.
     */
    virtual void close() = 0;

    /*!
     * Returns a saved position where this plan execution state is at. A call
     * to `rewind(DatumRef)` with the saved position will rewind the execution
     * state to the same as its current state. Calling `get_record()` on the
     * rewinded object will return the same values for the same record at the
     * time the `save_position()` is called. The returned saved position must
     * not be null.
     *
     * An operator that does not support rewinding to a saved position should
     * never override this function.
     */
    virtual Datum
    save_position() const {
        LOG(kFatal, "save_position() not supported by %s",
                    node_tag_name(get_tag()));
        return Datum::FromNull();
    }

    /*!
     * Rewind the execution state as if it has just been initialized. Thus,
     * after this call, `next_tuple()` will trigger another round of scan from
     * the very beginning. Calling `rewind()` should have the same effect as
     * calling `close()` followed by another `init()`, but is usually a more
     * efficient because it may avoid certain memory deallocation/allocation or
     * recomputation.
     *
     * Note that an operator must support this overload of rewind() even if it
     * does not support rewinding to a saved position.
     */
    virtual void rewind() = 0;

    /*!
     * Rewinds the execution state to the same as the one that was previously
     * saved and returns if the rewinded state is currently at a valid record.
     * Calling `get_record()` on the rewinded object will return the same
     * values for the same record at the time the the \p saved_position was
     * returned by `save_position()`.
     *
     * It is undefined if this function is passed a \p saved_position that was
     * not returned by this plan execution state object.
     */
    virtual bool
    rewind(DatumRef saved_position) {
        LOG(kFatal, "rewind(saved_position) not supported by %s",
                    node_tag_name(get_tag()));
        return false;
    }

    /*!
     * Returns the corresponding plan.
     */
    virtual const PlanNode* get_plan() const = 0;

protected:
    /*!
     * Utility function to get the raw pointer of \p i-th child of this
     * execution state.
     */
    PlanExecState* get_child(size_t i) const {
        return get_input_as<PlanExecState>(i);
    }

    bool m_initialized;
};

}    // namespace taco

#endif
