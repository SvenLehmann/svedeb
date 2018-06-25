package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.MaterializationWorker.{MaterializationWorkerState, MaterializeTable, MaterializedTable}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Table.{ActorsForColumn, ColumnList, GetColumnFromTable, ListColumnsInTable}

object MaterializationWorker {
  def props(api: ActorRef, user: ActorRef): Props = Props(new MaterializationWorker(api, user))

  case class MaterializeTable(table: ActorRef)
  case class MaterializedTable(user: ActorRef, columns: Map[String, ColumnType])

  private case class MaterializationWorkerState(columnCount: Option[Int],
                                        data: Map[Int, Map[String, ColumnType]]) {
    def addResult(partitionId: Int, columnName: String, values: ColumnType): MaterializationWorkerState = {
      val partitionMap = data(partitionId)
      val updatedPartition = partitionMap + (columnName -> values)
      val updatedMap = data + (partitionId -> updatedPartition)
      MaterializationWorkerState(columnCount, updatedMap)
    }

    def setUpPartitions(partitionIds: Seq[Int]): MaterializationWorkerState = {
      val partitions = partitionIds.map(partitionId => partitionId -> Map.empty[String, ColumnType])
      // maps:_* expands the Seq to a variable args argument
      MaterializationWorkerState(columnCount, Map(partitions:_*))
    }

    def setColumnCount(columnCount: Int): MaterializationWorkerState = {
      MaterializationWorkerState(Some(columnCount), data)
    }

    def hasFinished: Boolean = {
        columnCount.isDefined &&
        !data.values.exists(partitionMap => partitionMap.size != columnCount.get)
    }

    def convertToResult(): Map[String, ColumnType] = {
      val aggregated = data.values.flatten.groupBy(_._1).mapValues( _.map(_._2).toSeq)
      aggregated.mapValues(_.reduce((l, r) => ColumnType(l.values ++ r.values)))
    }
  }
}

class MaterializationWorker(api: ActorRef, user: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(MaterializationWorkerState(None, Map.empty))

  private def fetchColumnNames(table: ActorRef): Unit = {
    log.debug("Fetching column names")
    table ! ListColumnsInTable()
  }

  private def fetchColumns(state: MaterializationWorkerState, table: ActorRef, columnNames: Seq[String]): Unit = {
    log.debug("Fetching column actors")
    val newState = state.setColumnCount(columnNames.size)
    context.become(active(newState))

    columnNames.foreach(name => table ! GetColumnFromTable(name))
  }

  private def fetchData(state: MaterializationWorkerState, columnActors: Map[Int, ActorRef]): Unit = {
    log.debug("Fetching data from column actors")
    val newState = state.setUpPartitions(columnActors.keys.toSeq)
    context.become(active(newState))

    columnActors.values.foreach(columnActor => columnActor ! ScanColumn())
  }

  private def saveScannedValues(state: MaterializationWorkerState,
                                partitionId: Int,
                                columnName: String,
                                values: ColumnType): Unit = {
    log.debug("Saving partial result for partition {} and column {}", partitionId, columnName)
    val newState = state.addResult(partitionId, columnName, values)
    context.become(active(newState))

    if (newState.hasFinished) {
      log.debug("Received all results")
      val data = newState.convertToResult()
      api ! MaterializedTable(user, data)
    }
  }

  private def active(state: MaterializationWorkerState): Receive = {
    case MaterializeTable(table) => fetchColumnNames(table)
    case ColumnList(columnNames) => fetchColumns(state, sender(), columnNames)
    case ActorsForColumn(_, columnActors) => fetchData(state, columnActors)
    case ScannedValues(partitionId, columnName, values) => saveScannedValues(state, partitionId, columnName, values)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
