package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.QueryPlanExecutor.{APIWorkerState, QueryFinished, Run}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.RowType

object QueryPlanExecutor {
  case class Run(queryPlan: Vector[QueryPlanNode])

  case class QueryFinished(resultTable: ActorRef)

  private case class APIWorkerState(stage: Int, queryPlan: Vector[QueryPlanNode], sender: ActorRef) {
    def increaseStage(): APIWorkerState = {
      APIWorkerState(stage + 1, queryPlan, sender)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new QueryPlanExecutor(tableManager))
}

/*
 * TODO: Save intermediate results as attribute in QueryPlanNode
 * TODO: Consider using common messages for invoking operators, e.g. Execute
 */
class QueryPlanExecutor(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(APIWorkerState(0, Vector(), ActorRef.noSender))

  def buildInitialOperator(state: APIWorkerState, queryPlan: Vector[QueryPlanNode]): Unit = {
    log.debug("Building initial operator")

    val newState = APIWorkerState(1, queryPlan, sender())
    context.become(active(newState))

    queryPlan(0) match {
      case GetTable(tableName: String) =>
        val getTableOperator = context.actorOf(GetTableOperator.props(tableManager, tableName))
        getTableOperator ! Execute()
      case CreateTable(tableName: String, columnNames: List[String]) =>
        val createTableOperator = context.actorOf(CreateTableOperator.props(tableManager, tableName, columnNames))
        createTableOperator ! Execute()
      case DropTable(tableName: String) =>
        val dropTableOperator = context.actorOf(DropTableOperator.props(tableManager, tableName))
        dropTableOperator ! Execute()
      case _ => throw new Exception("Incorrect first operator")
    }
  }

  def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {
    log.debug("handling query result")
    if (state.stage == state.queryPlan.length) {
      state.sender ! QueryFinished(resultTable)
      return
    }
    val nextJob = state.queryPlan(state.stage)
    val newState = state.increaseStage()
    context.become(active(newState))

    nextJob match {
      case Scan(columnName: String, predicate: (String => Boolean)) =>
        val scanOperator = context.actorOf(ScanOperator.props(resultTable, columnName, predicate))
        scanOperator ! Execute()
      case InsertRow(row: RowType) =>
        val scanOperator = context.actorOf(InsertRowOperator.props(resultTable, row))
        scanOperator ! Execute()
      case _ => throw new Exception("Incorrect operator")
    }
  }

  private def active(state: APIWorkerState): Receive = {
    case Run(queryPlan) => buildInitialOperator(state, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
  }
}
