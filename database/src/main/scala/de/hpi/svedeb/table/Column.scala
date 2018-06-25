package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.svedeb.table.Column._
import de.hpi.svedeb.utils.Utils.{RowId, ValueType}

object Column {
  case class AppendValue(value: ValueType)
  case class FilterColumn(predicate: ValueType => Boolean)
  // None returns all values
  case class ScanColumn(indices: Option[Seq[RowId]] = None)
  case class GetColumnName()
  case class GetColumnSize()

  // Result events
  case class FilteredRowIndices(partitionId: Int, columnName: String, indices: Seq[RowId])
  case class ScannedValues(partitionId: Int, columnName: String, values: ColumnType)
  case class ValueAppended(partitionId: Int, columnName: String)
  case class ColumnName(name: String)
  case class ColumnSize(partitionId: Int, size: Int)

  def props(partitionId: Int,
            columnName: String,
            values: ColumnType = ColumnType()): Props = Props(new Column(partitionId, columnName, values))
}

class Column(partitionId: Int, columnName: String, initialValues: ColumnType) extends Actor with ActorLogging {
  override def receive: Receive = active(initialValues)

  private def filter(values: ColumnType, predicate: ValueType => Boolean): Unit = {
    val filteredIndices = values.filterByPredicate(predicate)
    sender() ! FilteredRowIndices(partitionId, columnName, filteredIndices)
  }

  private def scan(values: ColumnType, indices: Option[Seq[RowId]]): Unit = {
    if (indices.isDefined) {
      val scannedValues = values.filterByIndices(indices.get)
      sender() ! ScannedValues(partitionId, columnName, scannedValues)
    } else {
      sender() ! ScannedValues(partitionId, columnName, values)
    }
  }

  private def addRow(values: ColumnType, value: ValueType): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values.append(value)))

    sender() ! ValueAppended(partitionId, columnName)
  }

  private def active(values: ColumnType): Receive = {
    case AppendValue(value) => addRow(values, value)
    case FilterColumn(predicate) => filter(values, predicate)
    case ScanColumn(indices) => scan(values, indices)
    case GetColumnName() => sender() ! ColumnName(columnName)
    case GetColumnSize() => sender() ! ColumnSize(partitionId, values.size())
    case m => throw new Exception(s"Message not understood: $m")
  }
}

