package de.hpi.svedeb.queryPlan

case class NestedLoopJoin(left: AbstractQueryPlanNode,
                          right: AbstractQueryPlanNode,
                          leftColumn: String,
                          rightColumn: String,
                          predicate: (String, String) => Boolean
                         ) extends Join(left, right, leftColumn, rightColumn, predicate)
