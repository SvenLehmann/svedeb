package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.QueryPlanExecutor.{APIWorkerState, QueryFinished, Run}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.RowType

object QueryPlanExecutor {
  case class Run(queryId: Int, queryPlan: QueryPlan)

  case class QueryFinished(queryId: Int, resultTable: ActorRef)

  private case class APIWorkerState(sender: ActorRef, queryPlan: Option[QueryPlan] = None, queryId: Option[Int] = None) {
    def storeQueryId(queryId: Int): APIWorkerState = {
      APIWorkerState(sender, queryPlan, Some(queryId))
    }
    def storeSender(sender: ActorRef): APIWorkerState = {
      APIWorkerState(sender, queryPlan, queryId)
    }

    def assignWorker(queryPlan: QueryPlan,
                     worker: ActorRef,
                     node: AbstractQueryPlanNode): APIWorkerState = {
      val newQueryPlan = queryPlan.findNodeAndUpdateWorker(node, worker)
      APIWorkerState(sender, Some(newQueryPlan), queryId)
    }

    def nextStage(queryPlan: QueryPlan,
                  nextStage: AbstractQueryPlanNode,
                  nextWorker: ActorRef): APIWorkerState = {
      val newQueryPlan = queryPlan.findNodeAndUpdateWorker(nextStage, nextWorker)
      APIWorkerState(sender, Some(newQueryPlan), queryId)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new QueryPlanExecutor(tableManager))
}

class QueryPlanExecutor(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(APIWorkerState(ActorRef.noSender))

  private def nodeToOperatorActor(node: AbstractQueryPlanNode, resultTable: Option[ActorRef] = None): ActorRef = {
    node match {
      case GetTable(tableName: String) =>
        context.actorOf(GetTableOperator.props(tableManager, tableName))
      case CreateTable(tableName: String, columnNames: List[String], partitionSize: Int) =>
        context.actorOf(CreateTableOperator.props(tableManager, tableName, columnNames, partitionSize))
      case DropTable(tableName: String) =>
        context.actorOf(DropTableOperator.props(tableManager, tableName))
      case Scan(_, columnName: String, predicate: (String => Boolean)) =>
        context.actorOf(ScanOperator.props(resultTable.get, columnName, predicate))
      case InsertRow(_, row: RowType) =>
        context.actorOf(InsertRowOperator.props(resultTable.get, row))
      case _ => throw new Exception("Unknown node type, cannot build operator")
    }
  }

  private def handleQuery(state: APIWorkerState, queryId: Int, queryPlan: QueryPlan): Unit = {
    log.debug("Building initial operator")
    val firstStage = queryPlan.findNextStage().get
    val operator = nodeToOperatorActor(firstStage)

    val newState = state.storeQueryId(queryId).storeSender(sender()).assignWorker(queryPlan, operator, firstStage)
    context.become(active(newState))

    operator ! Execute()
  }

  private def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {
    log.debug("Handling query result")
    if (state.queryPlan.isEmpty) {
      throw new Exception("No query plan to execute")
    }

    val queryPlan = state.queryPlan.get

    queryPlan.saveIntermediateResult(sender(), resultTable)
    val nextStage = queryPlan.findNextStage()

    if (nextStage.isEmpty) {
//      TODO: Add test that verifies deletion of operators
      queryPlan.cleanUpOperators()
      state.sender ! QueryFinished(state.queryId.get, resultTable)
    } else {
      val operator = nodeToOperatorActor(nextStage.get, Some(resultTable))

      val newState = state.nextStage(state.queryPlan.get, nextStage.get, operator)
      context.become(active(newState))

      operator ! Execute()
    }
  }

  private def active(state: APIWorkerState): Receive = {
    case Run(queryId, queryPlan) => handleQuery(state, queryId, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
