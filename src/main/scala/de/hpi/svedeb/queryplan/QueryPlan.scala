package de.hpi.svedeb.queryplan

import akka.actor.ActorRef
import de.hpi.svedeb.table.RowType

object QueryPlan {
  abstract class QueryPlanNode()
  case class GetTable(tableName: String) extends QueryPlanNode
  case class Scan(columnName: String, predicate: String => Boolean) extends QueryPlanNode
  case class CreateTable(tableName: String, columnNames: Seq[String]) extends QueryPlanNode
  case class DropTable(tableName: String) extends QueryPlanNode
  case class InsertRow(row: RowType) extends QueryPlanNode

  // case class Join(left: QueryPlanNode, right: QueryPlanNode, ...) extends QueryPlanNode
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
