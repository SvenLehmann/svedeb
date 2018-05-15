package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.routing.RoundRobinPool
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.api.MaterializationWorker.{MaterializeTable, MaterializedTable}
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, Run}
import de.hpi.svedeb.queryplan.QueryPlan.QueryPlanNode
import de.hpi.svedeb.table.ColumnType

object API {
  case class Query(queryPlan: QueryPlanNode)
  case class Materialize(table: ActorRef)
  case class Shutdown()

  case class MaterializedResult(result: Map[String, ColumnType])
  case class Result(resultTable: ActorRef)

  private case class ApiState(queryCounter: Int = 0, runningQueries: Map[Int, ActorRef] = Map.empty) {
    def addQuery(sender: ActorRef): (ApiState, Int) = {
      val queryId = queryCounter + 1
      val newState = ApiState(queryId, runningQueries + (queryId -> sender))
      (newState, queryId)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new API(tableManager))
}

class API(tableManager: ActorRef) extends Actor with ActorLogging {
  private val workerActors: ActorRef = context.actorOf(RoundRobinPool(5).props(QueryPlanExecutor.props(tableManager)), "QueryExecutorRouter")

  override def receive: Receive = active(ApiState())

  private def materializeTable(user: ActorRef, resultTable: ActorRef): Unit = {
    val worker = context.actorOf(MaterializationWorker.props(self, user))
    worker ! MaterializeTable(resultTable)
  }

  private def handleShutdown() = {
    self ! PoisonPill
    tableManager ! PoisonPill
    context.system.terminate()
  }

  private def active(state: ApiState): Receive = {
    case Materialize(table) => materializeTable(sender(), table)
    case MaterializedTable(user, columns) => user ! MaterializedResult(columns)
    case Query(queryPlan) =>
      val (newState, queryId) = state.addQuery(sender())
      context.become(active(newState))
      workerActors ! Run(queryId, queryPlan)
    case QueryFinished(queryId, resultTable) =>
      state.runningQueries(queryId) ! Result(resultTable)
    case Shutdown() => handleShutdown()
    case m => throw new Exception("Message not understood: " + m)
  }


}
