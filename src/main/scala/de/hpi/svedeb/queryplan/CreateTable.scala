package de.hpi.svedeb.queryplan

case class CreateTable(tableName: String, columnNames: Seq[String], partitionSize: Int) extends AbstractQueryPlanNode(None, None)
