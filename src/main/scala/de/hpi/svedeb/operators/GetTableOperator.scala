package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperatorWorker.QueryResult
import de.hpi.svedeb.operators.GetTableOperator.{GetTable, GetTableOperatorState}

object GetTableOperator {
  case class GetTable(tableName: String)

  case class GetTableOperatorState(sender: ActorRef)

  def props(tableManager: ActorRef): Props = Props(new GetTableOperator(tableManager))
}

class GetTableOperator(tableManager: ActorRef) extends Actor with ActorLogging {

  def fetchTable(tableName: String): Unit = {
    log.debug("Fetching table {}", tableName)

    val newState = GetTableOperatorState(sender())
    context.become(active(newState))

    tableManager ! FetchTable(tableName)
  }

  override def receive: Receive = active(GetTableOperatorState(null))

  def handleResult(state: GetTableOperatorState, table: ActorRef): Unit = {
    log.debug("Received result table")
    state.sender ! QueryResult(table)
  }

  def active(state: GetTableOperatorState): Receive = {
    case GetTable(tableName) => fetchTable(tableName)
    case TableFetched(table) => handleResult(state, table)
  }
}
