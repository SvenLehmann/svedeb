package de.hpi.svedeb.queryPlan

case class Scan(input: AbstractQueryPlanNode,
                columnName: String,
                predicate: String => Boolean) extends AbstractQueryPlanNode(Some(input))
