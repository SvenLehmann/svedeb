package de.hpi.svedeb.table

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import de.hpi.svedeb.table.Partition.{AddColumn, GetColumn}
import de.hpi.svedeb.table.Table._

object Table {

  def props(columns: List[String], partitionSize: Int): Props = Props(new Table(columns, partitionSize))

  case class AddColumnToTable(name: String)
  case class AddRowToTable(row: List[String])
  case class ListColumnsInTable()
  case class GetColumnFromTable(columnName: String)
  case class GetPartitions()

  // Result events
  case class ColumnAddedToTable()
  case class RowAddedToTable()
  case class ColumnList(columns: List[String])
  case class ActorsForColumn(columnActors: List[ActorRef])
  case class PartitionsInTable(partitions: List[ActorRef])
}

class Table(columns: List[String], partitionSize: Int) extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = active(List.empty[ActorRef])

  private def active(partitions: List[ActorRef]): Receive = {
    case AddColumnToTable(name) => sender() ! addColumn(partitions, name)
    case AddRowToTable(row) => addRow(partitions, row)
    case ListColumnsInTable => sender() ! listColumns()
    case GetColumnFromTable(columnName) => sender() ! getColumns(partitions, columnName)
    case GetPartitions => sender() ! PartitionsInTable(partitions)
    case x => log.error("Message not understood: {}", x)
  }

  private def listColumns(): ColumnList = {
    log.info("Listing columns: {}", columns)
    ColumnList(columns)
  }

  private def getColumns(partitions: List[ActorRef], columnName: String): List[ActorRef] = ???

  private def addPartition(partitions: List[ActorRef]): List[ActorRef] = {
    val newPartition = context.actorOf(Partition.props())
    newPartition :: partitions
  }

  private def addColumn(partitions: List[ActorRef], name: String): ColumnAddedToTable = {
    partitions.foreach(p => p ! AddColumn(name))
    ColumnAddedToTable()
  }

  private def addRow(partitions: List[ActorRef], row: List[String]): Unit = {
    // TODO: decide which partition to publish to
    log.debug("Going to add row to table: {}", row)

    if (partitions.isEmpty) {
      log.debug("No partitions - going to create first partition")
      context.become(active(addPartition(partitions)))

      // Retry to add row
      self ! AddRowToTable(row)
    } else {
      log.debug("Append to head of partitions")
      // Least recently used partition is the head of the list
      // Let partition respond with `PartitionFullMessage` if partition is full
      partitions.head ! AddRowToTable(row)
    }
  }
}


