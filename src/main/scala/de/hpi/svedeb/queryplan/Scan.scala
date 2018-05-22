package de.hpi.svedeb.queryplan

case class Scan(input: AbstractQueryPlanNode, columnName: String, predicate: String => Boolean) extends AbstractQueryPlanNode(Some(input), None)
