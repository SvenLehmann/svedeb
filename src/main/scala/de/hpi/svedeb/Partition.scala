package de.hpi.svedeb

import akka.actor.Actor

case class ColumnListMessage(columns: List[String])
case class GetColumnsMessage()
case class AddColumnMessage(name: String)
case class DropColumnMessage(name: String)

class Partition extends Actor {

  override def receive: Receive = active(List.empty[String])

  def active(columns: List[String]): Receive = {
    case GetColumnsMessage => sender ! ColumnListMessage(columns)
    case AddColumnMessage(name) => addColumn(columns, name)
    case DropColumnMessage(name) => dropColumn(columns, name)
  }

  def addColumn(columns: List[String], name: String): Unit = {
    context.become(active(name :: columns))
  }

  def dropColumn(columns: List[String], name: String): Unit = {
    context.become(active(columns.filter(_ == name)))
  }
}
