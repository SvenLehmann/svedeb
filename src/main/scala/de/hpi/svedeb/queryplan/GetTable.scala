package de.hpi.svedeb.queryplan

case class GetTable(tableName: String) extends AbstractQueryPlanNode(None, None)
