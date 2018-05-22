package de.hpi.svedeb.queryplan

case class Join(left: AbstractQueryPlanNode, right: AbstractQueryPlanNode) extends AbstractQueryPlanNode(Some(left), Some(right))
