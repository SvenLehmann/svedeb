package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{RemoveTable, TableRemoved}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.DropTableOperator.State

object DropTableOperator {
  def props(tableManager: ActorRef, tableName: String): Props = Props(new DropTableOperator(tableManager, tableName))

  private case class State(sender: ActorRef)
}

class DropTableOperator(tableManager: ActorRef, tableName: String) extends AbstractOperator {
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
