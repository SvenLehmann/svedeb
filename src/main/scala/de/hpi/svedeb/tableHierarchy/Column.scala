package de.hpi.svedeb.tableHierarchy

import akka.actor.Actor

class Column extends Actor {
  override def receive: Receive = {
    case AddLineToColumnMessage(value: String) => addLine(value)
    case _ => print("Message not understood")
  }

  def addLine(value: String): Unit = {
    print("Stored value: " + value)
  }
}

case class AddLineToColumnMessage(value: String)
