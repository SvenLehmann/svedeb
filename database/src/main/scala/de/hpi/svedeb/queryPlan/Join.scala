package de.hpi.svedeb.queryPlan

import de.hpi.svedeb.utils.Utils.ValueType

abstract class Join(left: AbstractQueryPlanNode,
                    right: AbstractQueryPlanNode,
                    leftColumn: String,
                    rightColumn: String,
                    predicate: (ValueType, ValueType) => Boolean
                   ) extends AbstractQueryPlanNode(Some(left), Some(right))
