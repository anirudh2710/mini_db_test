// This is a list of node tags in the system.
//
// To define a new tag use the following macro:
//#define   NODE_TAG(clsname, base_clsname, optional_group_base_number)
//
// The node tags are organized into up to 16 groups, numbered by the highest 4
// bits of the tag number. Group 0 are uncategorized tree nodes, while others
// are assigned to parsing tree, logical plan, physical plan and etc. To start
// a new group with a base class A, add a line NODE_TAG(A, TreeNode, i << 12),
// where i is the group number. i must be consecutive and in increasing order.
// All node tags that belong to the same group must follow the node tag of the
// base class, and does not have the third argument set, e.g.,
// NODE_TAG(HeapScan, PlanNode) must appear between NODE_TAG(PlanNode,
// TreeNode, 1) and NODE_TAG(PlanExecState, TreeNode, 2).
//
// This list is currently manually created. TODO this is very error-prone. We
// need a script to automatically re-generate the list.
//

NODE_TAG(PlanNode, TreeNode, 0x1000)
NODE_TAG(TableScan, PlanNode)
NODE_TAG(Selection, PlanNode)
NODE_TAG(Projection, PlanNode)
NODE_TAG(IndexScan, PlanNode)
NODE_TAG(TempTable, PlanNode)
NODE_TAG(TableInsert, PlanNode)
NODE_TAG(TableDelete, PlanNode)
NODE_TAG(CartesianProduct, PlanNode)
NODE_TAG(Aggregation, PlanNode)
NODE_TAG(Sort, PlanNode)
NODE_TAG(Limit, PlanNode)
NODE_TAG(MergeJoin, PlanNode)
NODE_TAG(IndexNestedLoop, PlanNode)

// NODE_TAG(SomePlanNode, PlanNode)
// NODE_TAG(SomeOtherPlanNode, PlanNode)

NODE_TAG(PlanExecState, TreeNode, 0x2000)
NODE_TAG(TableScanState, PlanExecState)
NODE_TAG(SelectionState, PlanExecState)
NODE_TAG(ProjectionState, PlanExecState)
NODE_TAG(IndexScanState, PlanExecState)
NODE_TAG(TempTableState, PlanExecState)
NODE_TAG(TableInsertState, PlanExecState)
NODE_TAG(TableDeleteState, PlanExecState)
NODE_TAG(CartesianProductState, PlanExecState)
NODE_TAG(AggregationState, PlanExecState)
NODE_TAG(SortState, PlanExecState)
NODE_TAG(LimitState, PlanExecState)
NODE_TAG(MergeJoinState, PlanExecState)
NODE_TAG(IndexNestedLoopState, PlanExecState)

// NODE_TAG(SomePlanExecNode, PlanExecNode)
// NODE_TAG(SomeOtherPlanExecNode, PlanExecNode)

NODE_TAG(ExprNode, TreeNode, 0x3000)
NODE_TAG(Literal, ExprNode)
NODE_TAG(Variable, ExprNode)
NODE_TAG(UnaryOperator, ExprNode)
NODE_TAG(BinaryOperator, ExprNode)
NODE_TAG(AndOperator, ExprNode)
NODE_TAG(OrOperator, ExprNode)
NODE_TAG(Cast, ExprNode)
NODE_TAG(FuncCallOperator, ExprNode)
