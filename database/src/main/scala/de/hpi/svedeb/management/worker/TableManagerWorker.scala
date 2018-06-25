package de.hpi.svedeb.management.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{AddPartition, PartitionCreated}
import de.hpi.svedeb.management.worker.TableManagerWorker.{Execute, PartitionsCreated, TableManagerWorkerState}
import de.hpi.svedeb.table.ColumnType

object TableManagerWorker {
  case class Execute()

  case class PartitionsCreated(partitionMap: Map[Int, ActorRef])

  def props(originalSender: ActorRef,
            tableName: String,
            partitionSize: Int,
            columnNames: Seq[String],
            remainingPartitions: Seq[(Int, Map[String, ColumnType], ActorRef)],
            existingPartitions: Map[Int, ActorRef]) : Props = Props(
                                                            new TableManagerWorker(originalSender,
                                                              tableName,
                                                              partitionSize,
                                                              columnNames,
                                                              remainingPartitions,
                                                              existingPartitions))

  private case class TableManagerWorkerState(tableManager: ActorRef, partitionMap: Map[Int, ActorRef]) {
    def addPartialResult(partitionId: Int, partition: ActorRef): TableManagerWorkerState = {
      TableManagerWorkerState(tableManager, partitionMap + (partitionId -> partition))
    }

    def storeSender(tableManager: ActorRef): TableManagerWorkerState = {
      TableManagerWorkerState(tableManager, partitionMap)
    }

    def isFinished: Boolean = {
      partitionMap.values.forall(_ != ActorRef.noSender)
    }
  }
}

class TableManagerWorker(originalSender: ActorRef,
                         tableName: String,
                         partitionSize: Int,
                         columnNames: Seq[String],
                         remainingPartitions: Seq[(Int, Map[String, ColumnType], ActorRef)],
                         existingPartitions: Map[Int, ActorRef]) extends Actor with ActorLogging{
  override def receive: Receive = {
    val resultMap = remainingPartitions.map{ case (partitionId, _, _) => (partitionId, ActorRef.noSender)}.toMap
    active(TableManagerWorkerState(ActorRef.noSender, resultMap ++ existingPartitions))
  }

  private def initializePartitionCreation(state: TableManagerWorkerState): Unit = {
    context.become(active(state.storeSender(sender())))
    remainingPartitions.foreach{ case (partitionId, partitionData, remoteTableManager) => remoteTableManager ! AddPartition(partitionId, partitionData, partitionSize)}
  }

  private def handlePartitionCreated(state: TableManagerWorkerState, partitionId: Int, partition:ActorRef): Unit = {
    val newState = state.addPartialResult(partitionId, partition)
    context.become(active(newState))
    if (newState.isFinished) {
      newState.tableManager ! PartitionsCreated(newState.partitionMap)
    }
  }

  private def active(state: TableManagerWorkerState): Receive = {
    case Execute() => initializePartitionCreation(state)
    case PartitionCreated(partitionId, partition) => handlePartitionCreated(state, partitionId, partition)
  }
}
