package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.svedeb.table.Column._

object Column {
  case class AppendValue(value: String)
  case class FilterColumn(predicate: String => Boolean)
  // None returns all values
  case class ScanColumn(indizes: Option[Seq[Int]] = None)
  case class GetColumnName()
  case class GetColumnSize()

  // Result events
  case class FilteredRowIndizes(partitionId: Int, columnName: String, indizes: Seq[Int])
  case class ScannedValues(partitionId: Int, columnName: String, values: ColumnType)
  case class ValueAppended(partitionId: Int, columnName: String)
  case class ColumnName(name: String)
  case class ColumnSize(partitionId: Int, size: Int)

  def props(partitionId: Int, columnName: String, values: ColumnType = ColumnType()): Props = Props(new Column(partitionId, columnName, values))
}

class Column(partitionId: Int, columnName: String, initialValues: ColumnType) extends Actor with ActorLogging {
  override def receive: Receive = active(initialValues)

  private def filter(values: ColumnType, predicate: String => Boolean): Unit = {
    val filteredIndizes = values.filterByPredicate(predicate)
    sender() ! FilteredRowIndizes(partitionId, columnName, filteredIndizes)
  }

  private def scan(values: ColumnType, indizes: Option[Seq[Int]]): Unit = {
    if (indizes.isDefined) {
      val scannedValues = values.filterByIndizes(indizes.get)
      sender() ! ScannedValues(partitionId, columnName, scannedValues)
    } else {
      sender() ! ScannedValues(partitionId, columnName, values)
    }
  }

  private def addRow(values: ColumnType, value: String): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values.append(value)))

    sender() ! ValueAppended(partitionId, columnName)
  }

  private def active(values: ColumnType): Receive = {
    case AppendValue(value: String) => addRow(values, value)
    case FilterColumn(predicate) => filter(values, predicate)
    case ScanColumn(indizes) => scan(values, indizes)
    case GetColumnName() => sender() ! ColumnName(columnName)
    case GetColumnSize() => sender() ! ColumnSize(partitionId, values.size())
    case m => throw new Exception("Message not understood: " + m)
  }
}

