package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.ScanOperator.{Scan, ScanState}
import de.hpi.svedeb.table.Column.ScannedValues
import de.hpi.svedeb.table.Table.{ActorsForColumn, ColumnList, GetColumnFromTable, ListColumnsInTable}
import de.hpi.svedeb.table.{Column, ColumnType}

object ScanOperator {
  case class Scan(tableName: String)

  def props(tableManager: ActorRef): Props = Props(new ScanOperator(tableManager))

  case class ScanState(sender: ActorRef,
                       table: ActorRef,
                       results: List[ColumnType] = List.empty[ColumnType]) {
    def addResult(columnPart: ColumnType): ScanState = {
      val newResults = results :+ columnPart
      ScanState(sender, table, newResults)
    }
  }
}

/*
 * 1. Get Table Actor
 * 2. Get Columns
 * 3. Scan
 * 4. Build result
 */
class ScanOperator(tableManager: ActorRef) extends AbstractOperatorWorker(tableManager) {
  override def receive: Receive = active(ScanState(null, null))

  private def fetchColumnNames(state: ScanState, tableRef: ActorRef): Unit = {
    val newState = ScanState(state.sender, tableRef, state.results)
    context.become(active(newState))

    tableRef ! ListColumnsInTable()
  }

  private def fetchColumns(state: ScanState, columnNames: Seq[String]): Unit = {
    columnNames.foreach(name => state.table ! GetColumnFromTable(name))
  }

  private def scanColumns(state: ScanState, actors: Seq[ActorRef]): Unit = {
    actors.foreach(actor => actor ! Column.ScanColumn(None))
  }

  private def saveColumnPart(state: ScanState, column: ColumnType): Unit = {
    val newState = state.addResult(column)
    context.become(active(newState))
  }

  private def scan(state: ScanState, table: String): Unit = {
    val newState = ScanState(sender(), null, state.results)
    context.become(active(newState))

    tableManager ! FetchTable(table)
  }

  private def active(state: ScanState): Receive = {
    case Scan(table) => scan(state, table)
    case TableFetched(tableRef) => fetchColumnNames(state, tableRef)
    case ColumnList(columnNames) => fetchColumns(state, columnNames)
    case ActorsForColumn(actors) => scanColumns(state, actors)
    case ScannedValues(name, column) => saveColumnPart(state, column)
  }
}
