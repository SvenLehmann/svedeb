package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.GetTableOperator.State

object GetTableOperator {
  def props(tableManager: ActorRef, tableName: String): Props = Props(new GetTableOperator(tableManager, tableName))

  private case class State(sender: ActorRef)
}

class GetTableOperator(tableManager: ActorRef, tableName: String) extends AbstractOperator {
  override def receive: Receive = active(State(ActorRef.noSender))

  private def fetchTable(): Unit = {
    log.debug("Fetching table {}", tableName)

    val newState = State(sender())
    context.become(active(newState))

    tableManager ! FetchTable(tableName)
  }

  private def handleResult(state: State, table: ActorRef): Unit = {
    log.debug("Received result table")
    state.sender ! QueryResult(table)
  }

  private def active(state: State): Receive = {
    case Execute() => fetchTable()
    case TableFetched(table) => handleResult(state, table)
    case m => throw new Exception("Message not understood: " + m)
  }
}
