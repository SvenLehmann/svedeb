package de.hpi.svedeb.table.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.table.Partition.{ColumnRetrieved, GetColumn}
import de.hpi.svedeb.table.worker.TableWorker.{GetColumnFromTableWorker, InternalActorsForColumn, TableWorkerState}

object TableWorker {
  case class GetColumnFromTableWorker(originalSender: ActorRef, columnName:String)

  case class InternalActorsForColumn(originalSender:ActorRef, columnName: String, columnActors: Seq[ActorRef])

  def props(partitions: Seq[ActorRef]): Props = Props(new TableWorker(partitions))

  private case class TableWorkerState(originalSender: ActorRef, tableSender: ActorRef, columns: Map[Int, ActorRef]) {
    def storeSender(originalSender: ActorRef, sender: ActorRef): TableWorkerState = {
      TableWorkerState(originalSender, sender, columns)
    }

    def addColumn(partitionId: Int, columnName: String, column: ActorRef): TableWorkerState = {
      val newColumns = columns + (partitionId -> column)
      TableWorkerState(originalSender, tableSender, newColumns)
    }

    def allColumnsRetrieved(columnName: String, numberOfPartitions: Int): Boolean = {
      columns.size == numberOfPartitions
    }

    def sortedColumns(): Seq[ActorRef] = {
      // sort by partition id to ensure order of column actors
      columns.toSeq.sortBy(_._1).map(_._2)
    }
  }
}

class TableWorker(partitions: Seq[ActorRef]) extends Actor with ActorLogging {
  override def receive: Receive = active(TableWorkerState(ActorRef.noSender, ActorRef.noSender, Map.empty))

  private def handleGetColumn(state: TableWorkerState, originalSender: ActorRef, columnName: String): Unit = {
    log.debug("Received get column job")
    val newState = state.storeSender(originalSender, sender())
    context.become(active(newState))
    if (partitions.isEmpty) {
      newState.tableSender ! InternalActorsForColumn(newState.originalSender, columnName, Seq.empty)
    } else {
      partitions.foreach(partition => partition ! GetColumn(columnName))
    }
  }

  private def handleColumnRetrieved(state: TableWorkerState, partitionId: Int, columnName: String, column: ActorRef) {
    log.debug("Received column actor")
    val newState = state.addColumn(partitionId, columnName, column)
    context.become(active(newState))

    if (newState.allColumnsRetrieved(columnName, partitions.size)) {
      log.debug("Received all column actors")
      newState.tableSender ! InternalActorsForColumn(newState.originalSender, columnName, newState.sortedColumns())
    }
  }

  private def active(state: TableWorkerState): Receive = {
    case GetColumnFromTableWorker(originalSender, columnName) => handleGetColumn(state, originalSender, columnName)
    case ColumnRetrieved(partitionId, columnName, column) => handleColumnRetrieved(state, partitionId, columnName, column)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
