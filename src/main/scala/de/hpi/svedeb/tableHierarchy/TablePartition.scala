package de.hpi.svedeb.tableHierarchy

import akka.actor.{Actor, ActorRef, Props}

class TablePartition extends Actor {
  var columns = Set[ActorRef]()

  override def receive: Receive = {
    case AddLineToPartitionMessage(line: String) => addLine(line)
    case _ => print("Message not understood")
  }

  def addLine(line: String) = {
    if (columns.isEmpty) {
      // TODO for every column in line
      val newColumn = context.actorOf(Props[Column])
      columns += newColumn
    }
    // TODO extract column name and add corresponding value
    columns.head ! AddLineToColumnMessage(line)
  }
}

case class AddLineToPartitionMessage(line: String)