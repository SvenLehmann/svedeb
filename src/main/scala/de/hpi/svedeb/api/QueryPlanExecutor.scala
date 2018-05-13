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
    /*def saveIntermediateResult(currentWorker: ActorRef, resultTable: ActorRef): APIWorkerState = {
      val newQueryPlan = queryPlan.saveIntermediateResult(currentWorker, resultTable)
      APIWorkerState(newQueryPlan, sender)
    }

    def assignWorker(worker: ActorRef, node: QueryPlanNode): APIWorkerState = {
      val newQueryPlan = queryPlan.updateAssignedWorker(worker, node)
      APIWorkerState(newQueryPlan, sender)
    }*/

    def assignWorkerAndSender(worker: ActorRef, node: QueryPlanNode, initialQueryPlan: QueryPlanNode, newSender: ActorRef): APIWorkerState = {
      val newQueryPlan = initialQueryPlan.updateAssignedWorker(worker, node)
      APIWorkerState(newQueryPlan, newSender)
    }

    def nextStage(lastWorker: ActorRef, resultTable: ActorRef, nextWorker: ActorRef, nextStep: QueryPlanNode): APIWorkerState = {
      val newQueryPlan = queryPlan.nextStage(lastWorker, resultTable, nextWorker, nextStep)
      APIWorkerState(newQueryPlan, sender)
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
        operator = context.actorOf(GetTableOperator.props(tableManager, tableName), name = "getTableOperator")
      case CreateTable(tableName: String, columnNames: List[String]) =>
        operator = context.actorOf(CreateTableOperator.props(tableManager, tableName, columnNames), name = "createTableOperator")
      case DropTable(tableName: String) =>
        operator = context.actorOf(DropTableOperator.props(tableManager, tableName), name = "dropTableOperator")
      case _ => throw new Exception("Incorrect first operator")
    }
    operator ! Execute()

    val newState = state.assignWorkerAndSender(operator, nextStep, queryPlan, sender())
    context.become(active(newState))
  }

  def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {
    log.debug("handling query result")

    val nextStep = state.queryPlan.findNextStepWithException(sender())
//    val nextStep = state.queryPlan.findNextStep()
    var operator = ActorRef.noSender
    nextStep match {
      case Scan(input: QueryPlanNode, columnName: String, predicate: (String => Boolean)) =>
        operator = context.actorOf(ScanOperator.props(resultTable, columnName, predicate))
      case InsertRow(table: QueryPlanNode, row: RowType) =>
        operator = context.actorOf(InsertRowOperator.props(resultTable, row))
      case EmptyNode() =>
        log.debug("sender: {}", state.sender)
        state.sender ! QueryFinished(resultTable)
        return
      case _ => throw new Exception("Incorrect operator")
    }
    operator ! Execute()
    // TODO: second sender is probably not correct here
    val newState = state.nextStage(sender(), resultTable, operator, nextStep)
    context.become(active(newState))
  }

  private def active(state: APIWorkerState): Receive = {
    case Run(queryPlan) => buildInitialOperator(state, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
  }
}
