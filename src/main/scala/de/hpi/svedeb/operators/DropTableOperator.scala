package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{AddTable, RemoveTable, TableAdded, TableRemoved}
import de.hpi.svedeb.operators.AbstractOperatorWorker.{Execute, QueryResult}
import de.hpi.svedeb.operators.CreateTableOperator.State

object DropTableOperator {
  def props(tableManager: ActorRef, tableName: String, columnNames: Seq[String]): Props = Props(new CreateTableOperator(tableManager, tableName, columnNames))

  case class State(sender: ActorRef)
}

class DropTableOperator(tableManager: ActorRef, tableName: String) extends AbstractOperatorWorker {
  override def receive: Receive = active(State(ActorRef.noSender))

  def execute(): Unit = {
    val newState = State(sender())
    context.become(active(newState))

    tableManager ! RemoveTable(tableName)
  }

  def handleTableAdded(state: State): Unit = {
    state.sender ! QueryResult(ActorRef.noSender)
  }

  def active(state: State): Receive = {
    case Execute() => execute()
    case TableRemoved() => handleTableAdded(state)
  }
}
