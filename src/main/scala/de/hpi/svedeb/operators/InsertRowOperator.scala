package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.InsertRowOperator.State
import de.hpi.svedeb.table.RowType
import de.hpi.svedeb.table.Table.{AddRowToTable, RowAddedToTable}

object InsertRowOperator {
  private case class State(sender: ActorRef)

  def props(table: ActorRef, row: RowType): Props = Props(new InsertRowOperator(table, row))
}

class InsertRowOperator(table: ActorRef, row: RowType) extends AbstractOperator {
  override def receive: Receive = active(State(ActorRef.noSender))

  def handleResult(state: State): Unit = {
    log.debug("Added row to table")
    state.sender ! QueryResult(table)
  }

  def insertRow(): Unit = {
    val newState = State(sender())
    context.become(active(newState))

    table ! AddRowToTable(row)
  }

  def active(state: State): Receive = {
    case Execute() => insertRow()
    case RowAddedToTable() => handleResult(state)
  }
}
