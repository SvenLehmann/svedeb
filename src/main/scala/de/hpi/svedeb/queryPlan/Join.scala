package de.hpi.svedeb.queryPlan

abstract class Join(left: AbstractQueryPlanNode,
                    right: AbstractQueryPlanNode,
                    leftColumn: String,
                    rightColumn: String,
                    predicate: (String, String) => Boolean
                   ) extends AbstractQueryPlanNode(Some(left), Some(right))
