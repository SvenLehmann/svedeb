package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{AddTable, TableAdded}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.CreateTableOperator.State

object CreateTableOperator {
  def props(tableManager: ActorRef, tableName: String, columnNames: Seq[String], partitionSize: Int): Props =
    Props(new CreateTableOperator(tableManager, tableName, columnNames, partitionSize))

  private case class State(sender: ActorRef)
}

class CreateTableOperator(tableManager: ActorRef,
                          tableName: String,
                          columnNames: Seq[String],
                          partitionSize: Int) extends AbstractOperator {
  override def receive: Receive = active(State(ActorRef.noSender))

  private def execute(): Unit = {
    val newState = State(sender())
    context.become(active(newState))

    tableManager ! AddTable(tableName, columnNames, partitionSize)
  }

  private def handleTableAdded(state: State, tableRef: ActorRef): Unit = {
    state.sender ! QueryResult(tableRef)
  }

  private def active(state: State): Receive = {
    case Execute() => execute()
    case TableAdded(tableRef) => handleTableAdded(state, tableRef)
    case m => throw new Exception(s"Message not understood: $m")
  }
}