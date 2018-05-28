package de.hpi.svedeb.queryPlan

case class CreateTable(tableName: String, columnNames: Seq[String], partitionSize: Int) extends AbstractQueryPlanNode(None, None)
