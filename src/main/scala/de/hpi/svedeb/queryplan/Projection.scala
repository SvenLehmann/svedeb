package de.hpi.svedeb.queryplan

case class Projection(input: AbstractQueryPlanNode, columns: Seq[String]) extends AbstractQueryPlanNode(Some(input), None)
