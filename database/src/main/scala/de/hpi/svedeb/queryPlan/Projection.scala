package de.hpi.svedeb.queryPlan

case class Projection(input: AbstractQueryPlanNode, columns: Seq[String]) extends AbstractQueryPlanNode(Some(input))
