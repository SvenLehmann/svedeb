package de.hpi.svedeb.table

import akka.actor.{Actor, Props}
import akka.event.Logging
import de.hpi.svedeb.table.Column._

object Column {
  case class AppendValue(value: String)
  case class Scan()
  case class GetColumnName()
  case class GetNumberOfRows()

  // Result events
  case class ScannedValues(values: List[String])
  case class ValueAppended()
  case class ColumnName(name: String)
  case class NumberOfRows(size: Int)

  def props(name: String): Props = Props(new Column(name))
}

class Column(name: String) extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = active(List.empty[String])

  private def active(values: List[String]): Receive = {
    case AppendValue(value: String) => addRow(values, value)
    case Scan() => sender() ! ScannedValues(values)
    case GetColumnName() => sender() ! ColumnName(name)
    case GetNumberOfRows() => sender() ! NumberOfRows(values.size)
    case x => log.error("Message not understood: {}", x)
  }

  private def addRow(values: List[String], value: String): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values :+ value))

    sender() ! ValueAppended()
  }
}

