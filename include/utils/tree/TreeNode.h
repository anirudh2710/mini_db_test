#ifndef UTILS_TREE_NODE_H
#define UTILS_TREE_NODE_H

#include "tdb.h"

#include "utils/misc.h"
#include "utils/tree/NodeTag.h"

namespace taco {

/*!
 * TreeNode is the base class of all tree structures in TDB (e.g., parsing
 * tree, logical plan, physical plan, query execution state). Each TreeNode can
 * have zero or more inputs, and it owns the objects by storing a unique_ptr to
 * it. TreeNode constructors constructors and destructors should only
 * allocate/deallocate memory and perform initialization that may not generate
 * an error (e.g., initializing fields with trivial constructors). Any
 * initialization that may generate an error must be done in a separate
 * function call, that communicates the error to the caller through the return
 * value. Similarly, clean-up of a node that may produce some error must be
 * done in a separate function other than the destructor with a return status.
 *
 * A TreeNode object is owned by another TreeNode or some function scope that
 * has a unique_ptr to it. As a result, a TreeNode object may only be explicitly
 * moved as a unique_ptr or referenced through a raw pointer. It may not be
 * shallow copied and the copy constructor and the copy assignment operator is
 * deleted. The owner of a node is responsible to keep it alive while there are
 * references to the object. That being said, the most common pattern is to
 * never delete objects unless the entire tree goes is being deleted. If you
 * need to update an input without changing the inputs of the input, most likely
 * you'll have to replace the entire subtree anyway (since we do not allow
 * shallow copies). The original node will be deleted when the entire tree is
 * deleted.
 *
 * Each TreeNode object is associated with a tag (NodeTag). It identifies
 * the type of the TreeNode object. We use the tag rather than RTTI to cast or
 * identify TreeNode -- that will probably be more flexible and faster at the
 * cost of an additional 8-byte overhead (2 bytes + padding) per node. TODO
 * implement IsA with hierachies?
 *
 * We use visitor pattern to traverse, copy, modify tree structures.
 * TODO add visitor implementation to TreeNode.
 *
 */
class TreeNode {
public:

    template<class ...UniquePtrs>
    TreeNode(NodeTag tag, UniquePtrs&& ...input):
        m_tag(tag),
        m_input() {
        m_input.reserve(sizeof...(UniquePtrs));
        emplace_back_parameter_pack(m_input,
                                    std::forward<UniquePtrs>(input)...);
    }

    virtual ~TreeNode() {}

    TreeNode(TreeNode&&) = default;
    TreeNode &operator=(TreeNode&&) = default;
    TreeNode(const TreeNode&) = delete;
    TreeNode &operator=(const TreeNode &) = delete;

    constexpr NodeTag
    get_tag() const {
        return m_tag;
    }

    TreeNode *
    get_input(size_t i) const {
        if (i < m_input.size())
            return m_input[i].get();
        return nullptr;
    }

    template<class Node>
    Node *
    get_input_as(size_t i) const {
        if (i < m_input.size())
            return static_cast<Node*>(m_input[i].get());
        return nullptr;
    }

    std::string
    to_string() const {
        std::string buf;
        node_to_string(buf, 0);
        return buf;
    }

    /*!
     * Prints the node contents to a string buffer. No indentation is added on
     * the first line, and all subsequent lines have an indentation of at least
     * indent * INDENT_SIZE spaces.
     */
    void node_to_string(std::string &buf, int indent) const;

    const char *node_name() const {
        return node_tag_name(get_tag());
    }

    virtual void node_properties_to_string(std::string &buf,
                                           int indent) const = 0;

    static void append_indent(std::string &buf, int indent);
private:
    const NodeTag m_tag;

    std::vector<std::unique_ptr<TreeNode>> m_input;


    static constexpr const int TO_STRING_INDENT_SIZE = 4;
};

}   // namespace taco

#endif  // UTILS_TREE_NODE_H
