package de.hpi.svedeb.table

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import de.hpi.svedeb.table.Partition._
import de.hpi.svedeb.table.Table.AddRowMessage

object Table {
  case class AddRowMessage(row: List[String])

  def props(): Props = Props(new Table())
}

class Table extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = active(List.empty[ActorRef])

  def active(partitions: List[ActorRef]): Receive = {
    case AddRowMessage(row: List[String]) => addRow(partitions, row)
    case _ => log.error("Message not understood")
  }

  def addRow(partitions: List[ActorRef], row: List[String]): Unit = {
    // TODO: decide which partition to publish to
    log.debug("Going to add row to table: {}", row)

    if (partitions.isEmpty) {
      log.debug("No partitions - going to create first partition")
      val newPartition = context.actorOf(Partition.props())
      context.become(active(newPartition :: partitions))

      // Retry to add row
      self ! AddRowMessage(row)
    } else {
      log.debug("Append to head of partitions")
      // Least recently used partition is the head of the list
      // Let partition respond with `PartitionFullMessage` if partition is full
      partitions.head ! AddRowToPartitionMessage(row)
    }
  }
}


