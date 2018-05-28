package de.hpi.svedeb.queryPlan

case class Join(left: AbstractQueryPlanNode, right: AbstractQueryPlanNode) extends AbstractQueryPlanNode(Some(left), Some(right))
