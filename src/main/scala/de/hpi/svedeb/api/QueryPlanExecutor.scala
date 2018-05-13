package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.QueryPlanExecutor.{APIWorkerState, QueryFinished, Run}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.RowType

object QueryPlanExecutor {
  case class Run(queryPlan: QueryPlanNode)

  case class QueryFinished(resultTable: ActorRef)

  private case class APIWorkerState(queryPlan: QueryPlanNode, sender: ActorRef) {
    def saveIntermediateResult(currentWorker: ActorRef, resultTable: ActorRef): APIWorkerState = {
      val newQueryPlan = queryPlan.saveIntermediateResult(currentWorker, resultTable)
      APIWorkerState(newQueryPlan, sender)
    }

    def assignWorker(worker: ActorRef, node: QueryPlanNode): APIWorkerState = {
      val newQueryPlan = node.updateAssignedWorker(worker)
      APIWorkerState(newQueryPlan, sender)
    }

    def assignWorkerAndSender(worker: ActorRef, node: QueryPlanNode, newSender: ActorRef): APIWorkerState = {
      val newQueryPlan = node.updateAssignedWorker(worker)
      APIWorkerState(newQueryPlan, newSender)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new QueryPlanExecutor(tableManager))
}

/*
 * TODO: Consider using common messages for invoking operators, e.g. Execute
 */
class QueryPlanExecutor(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(APIWorkerState(EmptyNode(), ActorRef.noSender))

  def buildInitialOperator(state: APIWorkerState, queryPlan: QueryPlanNode): Unit = {
    val nextStep = queryPlan.findNextStep()
    var operator: ActorRef = ActorRef.noSender
    nextStep match {
      case GetTable(tableName: String) =>
        operator = context.actorOf(GetTableOperator.props(tableManager, tableName), name = "tableOperator")
      case CreateTable(tableName: String, columnNames: List[String]) =>
        operator = context.actorOf(CreateTableOperator.props(tableManager, tableName, columnNames))
      case DropTable(tableName: String) =>
        operator = context.actorOf(DropTableOperator.props(tableManager, tableName))
      case _ => throw new Exception("Incorrect first operator")
    }
    operator ! Execute()
    val newState = state.assignWorkerAndSender(operator, queryPlan, sender())
    context.become(active(newState))
  }

  def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {
    log.debug("handling query result")

    val newState = state.saveIntermediateResult(sender(), resultTable)
    context.become(active(newState))

//    val nextStep = state.queryPlan.findNextStepWithException(sender())
    val nextStep = state.queryPlan.findNextStep()
    nextStep match {
      case Scan(input: QueryPlanNode, columnName: String, predicate: (String => Boolean)) =>
        val scanOperator = context.actorOf(ScanOperator.props(resultTable, columnName, predicate))
        scanOperator ! Execute()
      case InsertRow(table: QueryPlanNode, row: RowType) =>
        val scanOperator = context.actorOf(InsertRowOperator.props(resultTable, row))
        scanOperator ! Execute()
      case EmptyNode() =>
        log.debug("sender: {}", state.sender)
        state.sender ! QueryFinished(state.queryPlan.resultTable)
      case _ => throw new Exception("Incorrect operator")
    }

    // TODO: not sure if that overrides the new state from before
    val newState2 = state.assignWorker(sender(), nextStep)
    context.become(active(newState2))
  }

  private def active(state: APIWorkerState): Receive = {
    case Run(queryPlan) => buildInitialOperator(state, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
  }
}
