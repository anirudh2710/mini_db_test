#ifndef EXPR_EXPRNODE_H
#define EXPR_EXPRNODE_H

#include "tdb.h"
#include "storage/Record.h"

#include "utils/tree/TreeNode.h"
#include "utils/tree/NodeTag.h"
#include "catalog/systables/Type.h"

namespace taco {

/*!
 * `ExprNode` is an abstract class of all expressions. It provides the basic
 * interface to construct, evaluate and derive return type of expressions. An
 * expression should be evaluated upon a record, where essential schema and type
 * information is passed through certain levels of the expression tree (e.g.
 * Variable, Literal, Cast, etc.).
 */
class ExprNode: public TreeNode {
public:
    /*!
     * Base constructor for all expression nodes. It is marked with a node
     * tag (see include/utils/tree/node_Tags.inc for more details) and unique
     * pointers to all of its children execution states. The unique pointers to
     * its children will be moved to `TreeNode` internal states after the
     * construction so you don't have to remember it separately.
     *
     * Note: You can use `TAG(ClassName)` to get the corresponding tag for a
     * plan execution state. For example, `TAG(Literal)` will give you
     * the tag of `Literal`.
     */
    template<class ...UniquePtrs>
    ExprNode(NodeTag tag, UniquePtrs&& ...input):
        TreeNode(tag, std::forward<UniquePtrs>(input)...) {}

    //! Deconstructor
    virtual ~ExprNode() {}

    //! Evaluate function upon a deserialized record.
    virtual Datum Eval(const std::vector<NullableDatumRef>& record) const = 0;

    //! Evaluate function on top of a serialized record.
    virtual Datum Eval(const char* record) const = 0;

    //! Derive the return type of this expression.
    std::shared_ptr<const SysTable_Type> ReturnType() const {
        return m_type;
    }

protected:
    std::shared_ptr<const SysTable_Type> m_type;
};

}

#endif      // QUERY_EXPR_EXPRNODE_H
