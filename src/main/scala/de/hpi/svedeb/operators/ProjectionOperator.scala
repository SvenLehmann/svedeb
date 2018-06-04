package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.ProjectionOperator.ProjectionState
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Table.{ActorsForColumn, GetColumnFromTable}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
import de.hpi.svedeb.utils.Utils

object ProjectionOperator {
  def props(input: ActorRef, columnNames: Seq[String]): Props = Props(new ProjectionOperator(input, columnNames))

  private case class ProjectionState(sender: ActorRef, result: Seq[Map[String, ColumnType]]) {
    def storeSender(sender: ActorRef): ProjectionState = {
      ProjectionState(sender, result)
    }

    /**
      * Stores and updates the output partition count
      * @param count the partition count
      * @return the updated state
      */
    def storeOutputPartitionCount(count: Int): ProjectionState = {
      // Prepare result maps to store intermediate results easily
      val newResult = (0 until count).map(_ => Map.empty[String, ColumnType])
      ProjectionState(sender, newResult)
    }

    def addPartialResult(partitionId: Int, columnName: String, values: ColumnType): ProjectionState = {
      val newResultMap = result(partitionId) + (columnName -> values)
      val newResult = result.updated(partitionId, newResultMap)
      ProjectionState(sender, newResult)
    }

    def isFinished(expectedColumnNames: Seq[String]): Boolean = {
      result.forall(partition => partition.keys.toSeq.sorted == expectedColumnNames.sorted)
    }
  }
}

class ProjectionOperator(input: ActorRef, columnNames: Seq[String]) extends AbstractOperator {
  override def receive: Receive = active(ProjectionState(ActorRef.noSender, Seq.empty))

  private def handleQuery(state: ProjectionState, sender: ActorRef): Unit = {
    log.debug("Handling Projection Query")
    val newState = state.storeSender(sender)
    context.become(active(newState))

    if (columnNames.isEmpty) {
      val result = createNewResultTable(state)
      sender ! QueryResult(result)
    } else {
      columnNames.foreach(name => input ! GetColumnFromTable(name))
    }
  }

  private def createNewResultTable(state: ProjectionState): ActorRef = {
    log.debug("Create result table")

    val partitions: Seq[ActorRef] = state.result.zipWithIndex
      .map { case (partitionResult, partitionId) =>
        context.actorOf(Partition.props(partitionId, partitionResult, Utils.defaultPartitionSize))
      }
    context.actorOf(Table.props(columnNames, Utils.defaultPartitionSize, partitions))
  }

  private def handleActorsForColumn(state: ProjectionState,
                                    columnName: String,
                                    actorsForColumns: Seq[ActorRef]): Unit = {
    log.debug(s"Handling actors for column $columnName")
    // Store partition count when first result is received
    if (state.result.isEmpty) {
      log.debug(s"Updating partition count to ${actorsForColumns.size}")
      val newState = state.storeOutputPartitionCount(actorsForColumns.size)
      context.become(active(newState))
    }

    actorsForColumns.foreach(columnActor => columnActor ! ScanColumn(None))
  }

  private def handleScannedValues(state: ProjectionState,
                                  partitionId: Int,
                                  columnName: String,
                                  values: ColumnType): Unit = {
    log.debug("Handling scanned values")
    val newState = state.addPartialResult(partitionId, columnName, values)
    context.become(active(newState))

    if (newState.isFinished(columnNames)) {
      log.debug("Query has finished")
      val table = createNewResultTable(newState)
      newState.sender ! QueryResult(table)
    }
  }

  private def active(state: ProjectionState): Receive = {
    case Execute() => handleQuery(state, sender())
    case ActorsForColumn(columnName, actorsForColumn) => handleActorsForColumn(state, columnName, actorsForColumn)
    case ScannedValues(partitionId, columnName, values) => handleScannedValues(state, partitionId, columnName, values)
  }
}
