package de.hpi.svedeb.queryPlan

import de.hpi.svedeb.table.ColumnType

//object CreateTable {
//  def apply(tableName: String, columnNames: Seq[String], partitionSize: Int): CreateTable =
//    CreateTable(tableName, Map(0 -> columnNames.map((_, ColumnType())).toMap), partitionSize)
//}

case class CreateTable(tableName: String, data: Map[Int, Map[String, ColumnType]], partitionSize: Int) extends AbstractQueryPlanNode() with Serializable
