package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.HashBucketEntry
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{HashJoinMaterializationWorkerState, MaterializeJoinResult, MaterializedJoinResult}
import de.hpi.svedeb.operators.workers.ProbeWorker.{FetchIndices, JoinedIndices}
import de.hpi.svedeb.table.Partition.{ScanColumns, ScannedColumns}
import de.hpi.svedeb.table.Table.{ColumnList, GetPartitions, ListColumnsInTable, PartitionsInTable}
import de.hpi.svedeb.table.{ColumnType, OptionalColumnType, Partition}
import de.hpi.svedeb.utils.Utils.ValueType

object HashJoinMaterializationWorker {
  case class MaterializeJoinResult()
  case class MaterializedJoinResult(hashKey: ValueType, partition: ActorRef)

  case class HashJoinMaterializationWorkerState(originalSender: ActorRef,
                                                leftPartitions: Option[Map[Int, ActorRef]],
                                                rightPartitions: Option[Map[Int, ActorRef]],
                                                leftQueriedPartitionCount: Option[Int],
                                                rightQueriedPartitionCount: Option[Int],
                                                leftValues: Map[Int, Map[String, OptionalColumnType]],
                                                rightValues: Map[Int, Map[String, OptionalColumnType]],
                                                joinedIndices: Seq[(HashBucketEntry, HashBucketEntry)]
                                               ) {
    def storeJoinedIndices(indices: Seq[(HashBucketEntry, HashBucketEntry)]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender,
        leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount,
        leftValues, rightValues, indices)
    }

    def storeQueriedPartitionCounts(leftQueriedPartitionCount: Int, rightQueriedPartitionCount: Int): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender,
        leftPartitions, rightPartitions,
        Some(leftQueriedPartitionCount), Some(rightQueriedPartitionCount),
        leftValues, rightValues, joinedIndices)
    }

    def storeOriginalSender(sender: ActorRef): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(sender,
        leftPartitions, rightPartitions,
        leftQueriedPartitionCount, rightQueriedPartitionCount,
        leftValues, rightValues, joinedIndices)
    }

    def receivedAllColumnsFromAllPartitions: Boolean = {
      leftValues.size == leftQueriedPartitionCount.get && rightValues.size == rightQueriedPartitionCount.get
    }

    def storeScannedColumns(sendingPartition: ActorRef, partitionId: Int, columns: Map[String, OptionalColumnType]): HashJoinMaterializationWorkerState = {
      if (leftPartitions.get.apply(partitionId) == sendingPartition) {
        HashJoinMaterializationWorkerState(
          originalSender, leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount,
          leftValues + (partitionId -> columns), rightValues, joinedIndices)
      } else if (rightPartitions.get.apply(partitionId) == sendingPartition) {
        HashJoinMaterializationWorkerState(
          originalSender, leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount,
          leftValues, rightValues + (partitionId -> columns), joinedIndices)
      } else {
        throw new Exception("booooom")
      }
    }

    def storeLeftPartitions(partitions: Map[Int, ActorRef]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender, Some(partitions), rightPartitions,
        leftQueriedPartitionCount, rightQueriedPartitionCount, leftValues, rightValues, joinedIndices)
    }

    def storeRightPartitions(partitions: Map[Int, ActorRef]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender, leftPartitions, Some(partitions),
        leftQueriedPartitionCount, rightQueriedPartitionCount, leftValues, rightValues, joinedIndices)
    }

    def hasReceivedBothPartitions: Boolean = {
      leftPartitions.isDefined && rightPartitions.isDefined
    }
  }

  def props(leftTable: ActorRef,
            rightTable: ActorRef,
            columnNames: (Seq[String], Seq[String]),
            hashKey: ValueType,
            probeWorker: ActorRef): Props =
    Props(new HashJoinMaterializationWorker(leftTable, rightTable, columnNames, hashKey, probeWorker))
}

class HashJoinMaterializationWorker(leftTable: ActorRef,
                                    rightTable: ActorRef,
                                    columnNames: (Seq[String], Seq[String]),
                                    hashKey: ValueType,
                                    probeWorker: ActorRef)
  extends Actor with ActorLogging {

  override def receive: Receive = active(
    HashJoinMaterializationWorkerState(ActorRef.noSender, None, None, None, None, Map.empty, Map.empty, Seq.empty))

  private def fetchIndices(state: HashJoinMaterializationWorkerState): Unit = {
    val newState = state.storeOriginalSender(sender())
    context.become(active(newState))

    probeWorker ! FetchIndices()
  }

  private def materializeJoinResult(state: HashJoinMaterializationWorkerState, indices: Seq[(HashBucketEntry, HashBucketEntry)]): Unit = {
    log.debug("Materialize Join Result")

    val newerState = state.storeJoinedIndices(indices)
    context.become(active(newerState))

    if (indices.isEmpty) {
      sender() ! MaterializedJoinResult(hashKey, ActorRef.noSender)
    } else {
      leftTable ! GetPartitions()
      rightTable ! GetPartitions()
    }
  }

  private def initiateMaterialization(state: HashJoinMaterializationWorkerState): Unit = {
    log.debug("Initiate Materialization")
    val leftGroupedByPartition = state.joinedIndices.map(_._1).groupBy(_.partitionId)
    val leftRowIdsPerPartition = leftGroupedByPartition.mapValues(_.map(_.rowId).distinct).map(identity)
    val rightGroupedByPartition = state.joinedIndices.map(_._2).groupBy(_.partitionId)
    val rightRowIdsPerPartition = rightGroupedByPartition.mapValues(_.map(_.rowId).distinct).map(identity)

    val newState = state.storeQueriedPartitionCounts(leftGroupedByPartition.size, rightGroupedByPartition.size)
    context.become(active(newState))

    leftRowIdsPerPartition.foreach {
      case (partitionId, rowIds) => state.leftPartitions.get.apply(partitionId) ! ScanColumns(rowIds)
    }
    rightRowIdsPerPartition.foreach {
      case (partitionId, rowIds) => state.rightPartitions.get.apply(partitionId) ! ScanColumns(rowIds)
    }
  }

  private def handlePartitionsInTable(state: HashJoinMaterializationWorkerState,
                                      partitions: Map[Int, ActorRef]): Unit = {
    log.debug("handlePartitionsInTable")
    val newState = if (sender() == leftTable) {
      state.storeLeftPartitions(partitions)
    } else if (sender() == rightTable) {
      state.storeRightPartitions(partitions)
    } else {
      throw new Exception("Boooom")
    }

    context.become(active(newState))

    if (newState.hasReceivedBothPartitions) {
      initiateMaterialization(newState)
    }
  }

  private def handleScannedColumns(state: HashJoinMaterializationWorkerState,
                                   partitionId: Int,
                                   columns: Map[String, OptionalColumnType]): Unit = {
    log.debug(s"partitionCounts ${state.leftQueriedPartitionCount.get} - ${state.rightQueriedPartitionCount.get}")
    log.debug(s"handle scanned columns with partitionId $partitionId & columns ${columns.keys}")

    val partitionSender = sender() // the partition, we use it to determine from which side it was sent
    val newState = state.storeScannedColumns(partitionSender, partitionId, columns)
    context.become(active(newState))

    if (newState.receivedAllColumnsFromAllPartitions) {
      val leftIndices = newState.joinedIndices.map(_._1)
      val rightIndices = newState.joinedIndices.map(_._2)

      def iterateIndices(columnName: String,
                         indices: Seq[HashBucketEntry],
                         values: Map[Int, Map[String, OptionalColumnType]]): ColumnType = {
        val columnValues = indices.map {
          case HashBucketEntry(pId, rowId, _) => values(pId).apply(columnName).values(rowId).get
        }.toIndexedSeq
        ColumnType(columnValues)
      }

      def reconstructColumns(columnNames: Seq[String],
                             indices: Seq[HashBucketEntry],
                             values: Map[Int, Map[String, OptionalColumnType]]): Map[String, ColumnType] = {
        columnNames.map(columnName => columnName -> iterateIndices(columnName, indices, values)).toMap
      }

      val leftColumns = reconstructColumns(columnNames._1, leftIndices, newState.leftValues)
      val rightColumns = reconstructColumns(columnNames._2, rightIndices, newState.rightValues)

      val partition = context.actorOf(Partition.props(hashKey, leftColumns ++ rightColumns))
      newState.originalSender ! MaterializedJoinResult(hashKey, partition)
    }
  }

  private def active(state: HashJoinMaterializationWorkerState): Receive = {
    case MaterializeJoinResult() => fetchIndices(state)
    case JoinedIndices(indices) => materializeJoinResult(state, indices)
    case PartitionsInTable(partitions) => handlePartitionsInTable(state, partitions)
    case ScannedColumns(partitionId, columns) => handleScannedColumns(state, partitionId, columns)
    case m => throw new Exception(s"Message not understood: $m")
  }

}
