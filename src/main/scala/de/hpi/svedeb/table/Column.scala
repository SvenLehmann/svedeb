package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.svedeb.table.Column._

object Column {
  case class AppendValue(value: String)
  case class Filter(predicate: String => Boolean)
  case class Scan(indizes: Option[Seq[Int]])
  case class GetColumnName()
  case class GetNumberOfRows()

  // Result events
  case class FilteredIndizes(indizes: Seq[Int])
  case class ScannedValues(values: ColumnType)
  case class ValueAppended()
  case class ColumnName(name: String)
  case class NumberOfRows(size: Int)

  def props(columnId: Int, name: String): Props = Props(new Column(columnId, name))
}

class Column(columnId: Int, name: String) extends Actor with ActorLogging {
  override def receive: Receive = active(ColumnType())

  def filter(values: ColumnType, predicate: String => Boolean): Unit = {
    val filteredIndizes = values.filterByPredicate(predicate)
    sender() ! FilteredIndizes(filteredIndizes)
  }

  def scan(values: ColumnType, indizes: Option[Seq[Int]]): Unit = {
    if (indizes.isDefined) {
      val scannedValues = values.filterByIndizes(indizes.get)
      sender() ! ScannedValues(scannedValues)
    } else {
      sender() ! ScannedValues(values)
    }
  }

  private def active(values: ColumnType): Receive = {
    case AppendValue(value: String) => addRow(values, value)
    case Filter(predicate) => filter(values, predicate)
    case Scan(indizes) => scan(values, indizes)
    case GetColumnName() => sender() ! ColumnName(name)
    case GetNumberOfRows() => sender() ! NumberOfRows(values.size())
    case x => log.error("Message not understood: {}", x)
  }

  private def addRow(values: ColumnType, value: String): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values.append(value)))

    sender() ! ValueAppended()
  }
}

