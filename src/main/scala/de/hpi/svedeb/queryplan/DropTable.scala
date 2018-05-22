package de.hpi.svedeb.queryplan

case class DropTable(tableName: String) extends AbstractQueryPlanNode(None, None)
