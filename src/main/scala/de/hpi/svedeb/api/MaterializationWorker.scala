package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.MaterializationWorker.MaterializeTable
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Table.{ActorsForColumn, ColumnList, GetColumnFromTable, ListColumnsInTable}

object MaterializationWorker {
  def props(api: ActorRef, user: ActorRef): Props = Props(new MaterializationWorker(api, user))

  case class MaterializeTable(table: ActorRef)
  case class MaterializedTable(sender: ActorRef, columns: Seq[ColumnType])
}

class MaterializationWorker(api: ActorRef, user: ActorRef) extends Actor with ActorLogging {

  def fetchColumnNames(table: ActorRef): Unit = {
    table ! ListColumnsInTable()
  }

  def fetchColumns(table: ActorRef, columnNames: Seq[String]): Unit = {
    columnNames.foreach(name => table ! GetColumnFromTable(name))
  }

  def fetchData(columnActors: Seq[ActorRef]): Unit = {
    columnActors.foreach(columnActor => columnActor ! ScanColumn(None))
  }

  override def receive: Receive = {
    case MaterializeTable(table) => fetchColumnNames(table)
    case ColumnList(columnNames) => fetchColumns(sender(), columnNames)
    case ActorsForColumn(columnActors) => fetchData(columnActors)
    case ScannedValues(columnName, values) =>
  }
}
