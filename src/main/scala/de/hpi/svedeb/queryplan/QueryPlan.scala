package de.hpi.svedeb.queryplan

object QueryPlan {
  abstract case class QueryPlanNode()
  case class GetTable(tableName: String) extends QueryPlanNode
  case class Scan(input: QueryPlanNode, columnName: String, predicate: String => Boolean) extends QueryPlanNode

  // case class Join(left: QueryPlanNode, right: QueryPlanNode, ...) extends QueryPlanNode
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
