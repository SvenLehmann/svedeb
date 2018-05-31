package de.hpi.svedeb.queryPlan

case class DropTable(tableName: String) extends AbstractQueryPlanNode(None, None, None, None)
