package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.QueryPlanExecutor.{APIWorkerState, QueryFinished, Run}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.{GetTableOperator, ScanOperator}
import de.hpi.svedeb.queryplan.QueryPlan.QueryPlanNode

object QueryPlanExecutor {
  case class Run(queryPlan: QueryPlanNode)

  case class QueryFinished(resultTable: ActorRef)

  private case class APIWorkerState(stage: Int, sender: ActorRef) {
    def increaseStage(): APIWorkerState = {
      APIWorkerState(stage + 1, sender)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new QueryPlanExecutor(tableManager))
}

/*
 * TODO: Translate QueryPlan to Operator hierarchy
 * TODO: Save intermediate results as attribute in QueryPlanNode
 * TODO: Consider using common messages for invoking operators, e.g. Execute
 */
class QueryPlanExecutor(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(APIWorkerState(0, ActorRef.noSender))

  def buildInitialOperator(state: APIWorkerState, queryPlan: Any): Unit = {
    log.debug("Building initial operator")

    val newState = APIWorkerState(1, sender())
    context.become(active(newState))

    // GetTable
    val getTableOperator = context.actorOf(GetTableOperator.props(tableManager, "SomeTable"))
    getTableOperator ! Execute()
  }

  def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {
    log.debug("handling query result")

    if (state.stage == 1) {
      // Scan
      val scanOperator = context.actorOf(ScanOperator.props(resultTable, "a", x => x == "Foo"))
      scanOperator ! Execute()
    } else if (state.stage == 2) {
      state.sender ! QueryFinished(resultTable)
    }

    val newState = state.increaseStage()
    context.become(active(newState))
  }

  private def active(state: APIWorkerState): Receive = {
    case Run(queryPlan) => buildInitialOperator(state, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
  }
}
