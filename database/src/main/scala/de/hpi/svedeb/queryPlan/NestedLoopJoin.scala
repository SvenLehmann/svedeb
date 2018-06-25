package de.hpi.svedeb.queryPlan

import de.hpi.svedeb.utils.Utils.ValueType

case class NestedLoopJoin(left: AbstractQueryPlanNode,
                          right: AbstractQueryPlanNode,
                          leftColumn: String,
                          rightColumn: String,
                          predicate: (ValueType, ValueType) => Boolean
                         ) extends Join(left, right, leftColumn, rightColumn, predicate)
