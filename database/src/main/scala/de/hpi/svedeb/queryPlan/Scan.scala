package de.hpi.svedeb.queryPlan

import de.hpi.svedeb.utils.Utils.ValueType

case class Scan(input: AbstractQueryPlanNode,
                columnName: String,
                predicate: ValueType => Boolean) extends AbstractQueryPlanNode(Some(input))
