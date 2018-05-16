package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.ProjectionOperator.ProjectionState
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Table.{ActorsForColumn, GetColumnFromTable}
import de.hpi.svedeb.table.{ColumnType, Table}
import de.hpi.svedeb.utils.Utils

object ProjectionOperator {
  def props(input: ActorRef, columnNames: Seq[String]): Props = Props(new ProjectionOperator(input, columnNames))

  private case class ProjectionState(sender: ActorRef = ActorRef.noSender, result: Vector[Map[String, ColumnType]] = Vector.empty) {
    def storeSender(sender: ActorRef): ProjectionState = {
      ProjectionState(sender, result)
    }

    def updatePartitionCount(count: Int): ProjectionState = {
      result = (0 until count).map(id => result(id))
    }

    def addPartialResult(partitionId: Int, columnName: String, values: ColumnType): ProjectionState = {
      val newResultMap = result + (columnName -> columnActors)
      ProjectionState(sender, newResultMap)
    }

    def isFinished(expectedColumnNames: Seq[String]): Boolean = {
      result.keys.toSeq.sorted == expectedColumnNames.sorted
    }
  }
}

class ProjectionOperator(input: ActorRef, columnNames: Seq[String]) extends AbstractOperator {
  override def receive: Receive = active(ProjectionState())

  def handleQuery(state: ProjectionState, sender: ActorRef): Unit = {
    val newState = state.storeSender(sender)
    context.become(active(newState))

    columnNames.foreach(name => input ! GetColumnFromTable(name))
  }

  def createNewResultTable(state: ProjectionState): ActorRef = {
    val columns = columnNames.map(name => state.result(name))
    val numberOfPartitions = columns.headOption.map(_.size).getOrElse(0)

    val partitions: Seq[ActorRef] = ???
    context.actorOf(Table.props(columnNames, Utils.defaultPartitionSize, partitions))
  }

  def handlePartialResult(state: ProjectionState, columnActors: Seq[ActorRef]): Unit = {
    if (state.result.isEmpty) {
      val newState = state.updatePartitionCount(columnActors.size)
      context.become(active(newState))
    }

    columnActors.foreach(columnActor => columnActor ! ScanColumn(None))
  }

  def handleScannedValues(state: ProjectionState, partitionId: Int, columnName: String, values: ColumnType): Unit = {
    val newState = state.addPartialResult(partitionId, columnName, values)
    context.become(active(newState))

    if (newState.isFinished(columnNames)) {
      val table = createNewResultTable(newState)
      newState.sender ! QueryResult(table)
    }
  }

  private def active(state: ProjectionState): Receive = {
    case Execute() => handleQuery(state, sender())
    case ActorsForColumn(_, columnActors) => handlePartialResult(state, columnActors)
    case ScannedValues(partitionId, columnName, values) => handleScannedValues(state, partitionId, columnName, values)
  }
}
