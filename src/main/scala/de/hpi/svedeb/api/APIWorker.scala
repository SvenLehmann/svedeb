package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.APIWorker.{APIWorkerState, Execute, QueryFinished}
import de.hpi.svedeb.operators.AbstractOperatorWorker.QueryResult
import de.hpi.svedeb.operators.{GetTableOperator, ScanOperator}
import de.hpi.svedeb.operators.ScanOperator.Scan

object APIWorker {
  case class Execute(queryPlan: Any)

  case class QueryFinished(resultTable: ActorRef)

  case class APIWorkerState(stage: Int, sender: ActorRef) {
    def increaseStage(): APIWorkerState = {
      APIWorkerState(stage + 1, sender)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new APIWorker(tableManager))
}

class APIWorker(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(APIWorkerState(0, null))


  def buildInitialOperator(state: APIWorkerState, queryPlan: Any): Unit = {
    val newState = APIWorkerState(1, sender())
    context.become(active(newState))

    // GetTable
    val getTableOperator = context.actorOf(GetTableOperator.props(tableManager))
  }

  def handleQueryResult(state: APIWorkerState, resultTable: ActorRef): Unit = {

    if (state.stage == 1) {
      // Scan
      val scanOperator = context.actorOf(ScanOperator.props(resultTable))
      scanOperator ! Scan("a", x => x == "Foo")
    } else if (state.stage == 2) {
      state.sender ! QueryFinished(resultTable)
    }

    val newState = state.increaseStage()
    context.become(active(newState))
  }

  private def active(state: APIWorkerState): Receive = {
    case Execute(queryPlan) => buildInitialOperator(state, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
  }
}
