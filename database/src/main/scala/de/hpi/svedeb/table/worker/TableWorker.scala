package de.hpi.svedeb.table.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.table.Partition.{ColumnRetrieved, GetColumn}
import de.hpi.svedeb.table.worker.TableWorker.{GetColumnFromTableWorker, InternalActorsForColumn, TableWorkerState}

object TableWorker {
  case class GetColumnFromTableWorker(originalSender: ActorRef, columnName:String)

  case class InternalActorsForColumn(originalSender:ActorRef, columnName: String, columnActors: Map[Int, ActorRef])

  def props(partitions: Map[Int, ActorRef]): Props = Props(new TableWorker(partitions))

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
  }
}

class TableWorker(partitions: Map[Int, ActorRef]) extends Actor with ActorLogging {
  override def receive: Receive = active(TableWorkerState(ActorRef.noSender, ActorRef.noSender, Map.empty))

  private def handleGetColumn(state: TableWorkerState, originalSender: ActorRef, columnName: String): Unit = {
    log.debug("Received get column job")
    val newState = state.storeSender(originalSender, sender())
    context.become(active(newState))

    if (partitions.isEmpty) {
      newState.tableSender ! InternalActorsForColumn(newState.originalSender, columnName, Map.empty)
    } else {
      partitions.values.foreach(partition => partition ! GetColumn(columnName))
    }
  }

  private def handleColumnRetrieved(state: TableWorkerState, partitionId: Int, columnName: String, column: ActorRef) {
    log.debug("Received column actor")
    val newState = state.addColumn(partitionId, columnName, column)
    context.become(active(newState))

    if (newState.allColumnsRetrieved(columnName, partitions.size)) {
      log.debug("Received all column actors")
      newState.tableSender ! InternalActorsForColumn(newState.originalSender, columnName, newState.columns)
    }
  }

  private def active(state: TableWorkerState): Receive = {
    case GetColumnFromTableWorker(originalSender, columnName) => handleGetColumn(state, originalSender, columnName)
    case ColumnRetrieved(partitionId, columnName, column) => handleColumnRetrieved(state, partitionId, columnName, column)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
