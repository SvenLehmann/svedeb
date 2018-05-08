package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{AddTable, TableAdded}
import de.hpi.svedeb.operators.AbstractOperatorWorker.{Execute, QueryResult}
import de.hpi.svedeb.operators.CreateTableOperator.State

object CreateTableOperator {
  def props(tableManager: ActorRef, tableName: String, columnNames: Seq[String]): Props = Props(new CreateTableOperator(tableManager, tableName, columnNames))

  case class State(sender: ActorRef)
}

class CreateTableOperator(tableManager: ActorRef, tableName: String, columnNames: Seq[String]) extends AbstractOperatorWorker {
  override def receive: Receive = active(State(ActorRef.noSender))

  def execute(): Unit = {
    val newState = State(sender())
    context.become(active(newState))

    tableManager ! AddTable(tableName, columnNames)
  }

  def handleTableAdded(state: State, tableRef: ActorRef): Unit = {
    state.sender ! QueryResult(tableRef)
  }

  def active(state: State): Receive = {
    case Execute() => execute()
    case TableAdded(tableRef) => handleTableAdded(state, tableRef)
  }
}