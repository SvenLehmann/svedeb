package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperatorWorker.{Execute, QueryResult}
import de.hpi.svedeb.operators.GetTableOperator.State

object GetTableOperator {
  private case class State(sender: ActorRef)

  def props(tableManager: ActorRef, tableName: String): Props = Props(new GetTableOperator(tableManager, tableName))
}

class GetTableOperator(tableManager: ActorRef, tableName: String) extends AbstractOperatorWorker {
  override def receive: Receive = active(State(ActorRef.noSender))

  def fetchTable(): Unit = {
    log.debug("Fetching table {}", tableName)

    val newState = State(sender())
    context.become(active(newState))

    tableManager ! FetchTable(tableName)
  }

  def handleResult(state: State, table: ActorRef): Unit = {
    log.debug("Received result table")
    state.sender ! QueryResult(table)
  }

  def active(state: State): Receive = {
    case Execute() => fetchTable()
    case TableFetched(table) => handleResult(state, table)
  }
}
