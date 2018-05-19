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

  private case class MaterializationWorkerState(partitionCount: Option[Int] = None,
                                        columnCount: Option[Int] = None,
                                        data: Map[Int, Map[String, ColumnType]] = Map.empty) {
    def addResult(partitionId: Int, columnName: String, values: ColumnType): MaterializationWorkerState = {
      val partitionMap = data(partitionId)
      val updatedPartition = partitionMap + (columnName -> values)
      val updatedMap = data + (partitionId -> updatedPartition)
      MaterializationWorkerState(partitionCount, columnCount, updatedMap)
    }

    def setPartitionCount(partitionCount: Int): MaterializationWorkerState = {
      val maps = (0 until partitionCount).map(partitionId => partitionId -> Map.empty[String, ColumnType])
      // maps:_* expands the Seq to a variable args argument
      MaterializationWorkerState(Some(partitionCount), columnCount, Map(maps:_*))
    }

    def setColumnCount(columnCount: Int): MaterializationWorkerState = {
      MaterializationWorkerState(partitionCount, Some(columnCount), data)
    }

    def hasFinished: Boolean = {
      partitionCount.isDefined &&
        columnCount.isDefined &&
        data.size == partitionCount.get &&
        !data.values.exists(partitionMap => partitionMap.size != columnCount.get)
    }

    def convertToResult(): Map[String, ColumnType] = {
      val aggregated = data.values.flatten.groupBy(_._1).mapValues( _.map(_._2).toSeq)
      aggregated.mapValues(_.reduce((l, r) => ColumnType(l.values ++ r.values)))
    }
  }
}

class MaterializationWorker(api: ActorRef, user: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(MaterializationWorkerState())

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

  private def fetchData(state: MaterializationWorkerState, columnActors: Seq[ActorRef]): Unit = {
    log.debug("Fetching data from column actors")
    val newState = state.setPartitionCount(columnActors.size)
    context.become(active(newState))

    columnActors.foreach(columnActor => columnActor ! ScanColumn())
  }

  private def saveScannedValues(state: MaterializationWorkerState, partitionId: Int, columnName: String, values: ColumnType): Unit = {
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
