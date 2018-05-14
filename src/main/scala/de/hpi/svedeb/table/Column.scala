package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.svedeb.table.Column._

object Column {
  case class AppendValue(value: String)
  case class FilterColumn(predicate: String => Boolean)
  // None returns all values
  case class ScanColumn(indizes: Option[Seq[Int]])
  case class GetColumnName()
  case class GetColumnSize()

  // Result events
  case class FilteredRowIndizes(indizes: Seq[Int])
  case class ScannedValues(columnName: String, values: ColumnType)
  case class ValueAppended()
  case class ColumnName(name: String)
  case class ColumnSize(size: Int)

  def props(partitionId: Int, name: String, values: ColumnType = ColumnType()): Props = Props(new Column(partitionId, name, values))
}

class Column(partitionId: Int, name: String, initialValues: ColumnType) extends Actor with ActorLogging {
  override def receive: Receive = active(initialValues)

  def filter(values: ColumnType, predicate: String => Boolean): Unit = {
    val filteredIndizes = values.filterByPredicate(predicate)
    sender() ! FilteredRowIndizes(filteredIndizes)
  }

  def scan(values: ColumnType, indizes: Option[Seq[Int]]): Unit = {
    if (indizes.isDefined) {
      val scannedValues = values.filterByIndizes(indizes.get)
      sender() ! ScannedValues(name, scannedValues)
    } else {
      sender() ! ScannedValues(name, values)
    }
  }

  private def active(values: ColumnType): Receive = {
    case AppendValue(value: String) => addRow(values, value)
    case FilterColumn(predicate) => filter(values, predicate)
    case ScanColumn(indizes) => scan(values, indizes)
    case GetColumnName() => sender() ! ColumnName(name)
    case GetColumnSize() => sender() ! ColumnSize(values.size())
    case x => log.error("Message not understood: {}", x)
  }

  private def addRow(values: ColumnType, value: String): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values.append(value)))

    sender() ! ValueAppended()
  }
}

