package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.QueryPlanExecutor.{APIWorkerState, QueryFinished, Run}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.RowType

object QueryPlanExecutor {
  case class Run(queryId: Int, queryPlan: QueryPlanNode)

  case class QueryFinished(queryId: Int, resultTable: ActorRef)

  private case class APIWorkerState(queryPlan: QueryPlanNode, sender: ActorRef, queryId: Option[Int] = None) {
    /*def saveIntermediateResult(currentWorker: ActorRef, resultTable: ActorRef): APIWorkerState = {
      val newQueryPlan = queryPlan.saveIntermediateResult(currentWorker, resultTable)
      APIWorkerState(newQueryPlan, sender)
    }

    def assignWorker(worker: ActorRef, node: QueryPlanNode): APIWorkerState = {
      val newQueryPlan = queryPlan.updateAssignedWorker(worker, node)
      APIWorkerState(newQueryPlan, sender)
    }*/

    def assignWorkerAndSender(worker: ActorRef, node: QueryPlanNode, queryId: Int, initialQueryPlan: QueryPlanNode, newSender: ActorRef): APIWorkerState = {
      val newQueryPlan = initialQueryPlan.updateAssignedWorker(worker, node)
      APIWorkerState(newQueryPlan, newSender, Some(queryId))
    }

    def nextStage(lastWorker: ActorRef, resultTable: ActorRef, nextWorker: ActorRef, nextStep: QueryPlanNode): APIWorkerState = {
      val newQueryPlan = queryPlan.nextStage(lastWorker, resultTable, nextWorker, nextStep)
      APIWorkerState(newQueryPlan, sender, this.queryId)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new QueryPlanExecutor(tableManager))
}

/*
 * TODO: Consider using common messages for invoking operators, e.g. Execute
 */
class QueryPlanExecutor(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(APIWorkerState(EmptyNode(), ActorRef.noSender))

  def buildInitialOperator(state: APIWorkerState, queryId: Int, queryPlan: QueryPlanNode): Unit = {
    val nextStep = queryPlan.findNextStep()
    // TODO: Consider using overloaded method, e.g. each QuerPlanNode returns its respective Props object
    val operator: ActorRef = nextStep match {
      case GetTable(tableName: String) =>
        context.actorOf(GetTableOperator.props(tableManager, tableName))
      case CreateTable(tableName: String, columnNames: List[String], partitionSize: Int) =>
        context.actorOf(CreateTableOperator.props(tableManager, tableName, columnNames, partitionSize), name = "createTableOperator")
      case DropTable(tableName: String) =>
        context.actorOf(DropTableOperator.props(tableManager, tableName), name = "dropTableOperator")
      case _ => throw new Exception("Incorrect first operator")
    }
    operator ! Execute()

    val newState = state.assignWorkerAndSender(operator, nextStep, queryId, queryPlan, sender())
    context.become(active(newState))
  }

  def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {
    log.debug("handling query result")

    val nextStep = state.queryPlan.findNextStepWithException(sender())
//    val nextStep = state.queryPlan.findNextStep()
    // TODO: Consider using overloaded method, e.g. each QuerPlanNode returns its respective Props object
    var operator = ActorRef.noSender
    nextStep match {
      case Scan(input: QueryPlanNode, columnName: String, predicate: (String => Boolean)) =>
        operator = context.actorOf(ScanOperator.props(resultTable, columnName, predicate))
      case InsertRow(table: QueryPlanNode, row: RowType) =>
        operator = context.actorOf(InsertRowOperator.props(resultTable, row))
      case EmptyNode() =>
        log.debug("sender: {}", state.sender)
        state.sender ! QueryFinished(state.queryId.get, resultTable)
        return
      case _ => throw new Exception("Incorrect operator")
    }
    val newState = state.nextStage(sender(), resultTable, operator, nextStep)
    context.become(active(newState))

    operator ! Execute()
  }

  private def active(state: APIWorkerState): Receive = {
    case Run(queryId, queryPlan) => buildInitialOperator(state, queryId, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
  }
}
