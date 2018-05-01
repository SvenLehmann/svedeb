package de.hpi.svedeb.table

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.Logging
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import de.hpi.svedeb.table.Partition.{ColumnList => _, _}
import de.hpi.svedeb.table.Table._

import scala.concurrent.Future

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

class Table(columns: List[String], partitionSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  // Initialize with single partition
  override def receive: Receive = {
    val newPartition = context.actorOf(Partition.props(columns, partitionSize), "partition0")
    active(List(newPartition))
  }

  private def active(partitions: List[ActorRef]): Receive = {
    case AddRowToTable(row) => addRow(partitions, row)
    case ListColumnsInTable() => sender() ! listColumns()
    case GetColumnFromTable(columnName) => pipe(getColumns(partitions, columnName)) to sender()
    case GetPartitions() => sender() ! PartitionsInTable(partitions)
    case x => log.error("Message not understood: {}", x)
  }

  private def listColumns(): ColumnList = {
    log.info("Listing columns: {}", columns)
    ColumnList(columns)
  }

  private def getColumns(partitions: List[ActorRef], columnName: String): Future[ActorsForColumn] = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    val listOfFutures = partitions.map(p => ask(p, GetColumn(columnName)).mapTo[RetrievedColumn])
    val foo = Future.sequence(listOfFutures)
    val bar = foo.map(list => list.map(c => c.column)).map(c => ActorsForColumn(c))
    bar
  }

  private def addRow(partitions: List[ActorRef], row: List[String]): Unit = {
    // TODO: decide which partition to publish to
    log.debug("Going to add row to table: {}", row)

    log.debug("Append to head of partitions")
    // Least recently used partition is the head of the list
    // Let partition respond with `PartitionFullMessage` if partition is full
    tryToAddRow(sender(), partitions, row)
  }

  private def tryToAddRow(sender: ActorRef, partitions: List[ActorRef], row: List[String]): Unit = {
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds) // needed for `ask` below

    val response = ask(partitions.head, AddRow(row))
    response.foreach {
      case RowAdded() =>
        log.info("Adding to existing partition")
        sender ! RowAddedToTable()
      case PartitionFull() =>
        log.info("Creating new partition")
        val newPartition = context.actorOf(Partition.props(columns, partitionSize), "partition" + partitions.size)
        val updatedPartitions = newPartition :: partitions
        context.become(active(updatedPartitions))
        tryToAddRow(sender, updatedPartitions, row)
    }
  }
}


