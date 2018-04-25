package de.hpi.svedeb.tableHierarchy

import akka.actor.{Actor, ActorRef, Props}

class Table extends Actor {
  var partitions = Set[ActorRef]()

  override def receive: Receive = {
    case AddLineMessage(line: String) => addLine(line)
    case _ => print("Message not understood")
  }

  def addLine(line: String): Unit = {
    // TODO decide which partition to publish to
    val newPartition = context.actorOf(Props[TablePartition])
    partitions += newPartition
    newPartition ! AddLineToPartitionMessage(line)
  }
}

case class AddLineMessage(line: String) //TODO find data format for lines
