package de.hpi.svedeb.table

import akka.actor.{Actor, Props}
import akka.event.Logging
import de.hpi.svedeb.table.Column.{AppendValue, DeleteValue, ScanValuesMessage, ValuesMessage}

object Column {
  case class AppendValue(value: String)
  case class DeleteValue(value: String)
  case class ScanValuesMessage()
  // Just for return values
  case class ValuesMessage(values: List[String])

  def props(name: String): Props = Props(new Column(name))
}

class Column(name: String) extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = active(List.empty[String])

  def active(values: List[String]): Receive = {
    case AppendValue(value: String) => addRow(values, value)
    case ScanValuesMessage => sender() ! ValuesMessage(values)
    case DeleteValue(value: String) => deleteRow(values, value)
    case _ => log.error("Message not understood")
  }

  def deleteRow(values: List[String], value: String): Unit = {
    log.debug("Going to delete value: {}", value)
    context.become(active(values.filter(_ != value)))
  }

  def addRow(values: List[String], value: String): Unit = {
    log.debug("Appending value: {}", value)
    context.become(active(values :+ value))
  }
}

