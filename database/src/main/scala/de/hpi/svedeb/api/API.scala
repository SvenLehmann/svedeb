package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.api.MaterializationWorker.{MaterializeTable, MaterializedTable}
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, Run}
import de.hpi.svedeb.queryPlan.QueryPlan
import de.hpi.svedeb.table.ColumnType

object API {
  case class Query(queryPlan: QueryPlan)
  case class Materialize(table: ActorRef)
  case class Shutdown()

  case class MaterializedResult(result: Map[String, ColumnType])
  case class Result(resultTable: ActorRef)

  private case class ApiState(queryCounter: Int, runningQueries: Map[Int, ActorRef]) {
    def addQuery(sender: ActorRef): (ApiState, Int) = {
      val queryId = queryCounter + 1
      val newState = ApiState(queryId, runningQueries + (queryId -> sender))
      (newState, queryId)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new API(tableManager))
}

class API(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(ApiState(0, Map.empty))

  private def materializeTable(user: ActorRef, resultTable: ActorRef): Unit = {
    val worker = context.actorOf(MaterializationWorker.props(self, user))
    worker ! MaterializeTable(resultTable)
  }

  private def handleShutdown() = {
    // TODO: Refactor, most likely the Poison Pill messages are not handled before the ActorSystem is shutdown.
    tableManager ! PoisonPill
    self ! PoisonPill
    context.system.terminate()
  }

  private def active(state: ApiState): Receive = {
    case Materialize(table) => materializeTable(sender(), table)
    case MaterializedTable(user, columns) => user ! MaterializedResult(columns)
    case Query(queryPlan) =>
      val (newState, queryId) = state.addQuery(sender())
      context.become(active(newState))
      val executor = context.actorOf(QueryPlanExecutor.props(tableManager))
      executor ! Run(queryId, queryPlan)
    case QueryFinished(queryId, resultTable) =>
      state.runningQueries(queryId) ! Result(resultTable)
    // Cannot kill sender() as it still holds reference to resulting TableActor. Only use for benchmarks
    // sender() ! PoisonPill
    case Shutdown() => handleShutdown()
    case m => throw new Exception(s"Message not understood: $m")
  }


}
