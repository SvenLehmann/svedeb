package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{HashJoinMaterializationWorkerState, MaterializeJoinResult, MaterializedJoinResult}
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
                                                leftColumnNames: Option[Seq[String]],
                                                rightColumnNames: Option[Seq[String]],
                                                leftValues: Map[Int, Map[String, OptionalColumnType]],
                                                rightValues: Map[Int, Map[String, OptionalColumnType]]
                                               ) {
    def storeQueriedPartitionCounts(leftQueriedPartitionCount: Int, rightQueriedPartitionCount: Int): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender,
        leftPartitions, rightPartitions,
        Some(leftQueriedPartitionCount), Some(rightQueriedPartitionCount),
        leftColumnNames, rightColumnNames,
        leftValues, rightValues)
    }

    def storeOriginalSender(sender: ActorRef): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(sender,
        leftPartitions, rightPartitions,
        leftQueriedPartitionCount, rightQueriedPartitionCount,
        leftColumnNames, rightColumnNames,
        leftValues, rightValues)
    }

    def receivedAllColumns: Boolean = {
      leftValues.size == leftQueriedPartitionCount.get && rightValues.size == rightQueriedPartitionCount.get
    }

    def storeScannedColumns(sendingPartition: ActorRef, partitionId: Int, columns: Map[String, OptionalColumnType]): HashJoinMaterializationWorkerState = {
      if (leftPartitions.get.apply(partitionId) == sendingPartition) {
        HashJoinMaterializationWorkerState(originalSender, leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount, leftColumnNames, rightColumnNames, leftValues + (partitionId -> columns), rightValues)
      } else if (rightPartitions.get.apply(partitionId) == sendingPartition) {
        HashJoinMaterializationWorkerState(originalSender, leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount, leftColumnNames, rightColumnNames, leftValues, rightValues + (partitionId -> columns))
      } else {
        throw new Exception("booooom")
      }
    }

    def storeLeftPartitions(partitions: Map[Int, ActorRef]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender, Some(partitions), rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount,leftColumnNames, rightColumnNames, leftValues, rightValues)
    }

    def storeRightPartitions(partitions: Map[Int, ActorRef]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender, leftPartitions, Some(partitions), leftQueriedPartitionCount, rightQueriedPartitionCount,leftColumnNames, rightColumnNames, leftValues, rightValues)
    }

    def storeLeftColumnNames(columnNames: Seq[String]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender, leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount,Some(columnNames), rightColumnNames, leftValues, rightValues)
    }

    def storeRightColumnNames(columnNames: Seq[String]): HashJoinMaterializationWorkerState = {
      HashJoinMaterializationWorkerState(originalSender, leftPartitions, rightPartitions, leftQueriedPartitionCount, rightQueriedPartitionCount,leftColumnNames, Some(columnNames), leftValues, rightValues)
    }

    def hasReceivedColumnsAndPartitions: Boolean = {
      leftPartitions.isDefined && rightPartitions.isDefined && leftColumnNames.isDefined && rightColumnNames.isDefined
    }
  }

  def props(leftTable: ActorRef,
            rightTable: ActorRef,
            hashKey: ValueType,
            indices: Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)]): Props =
    Props(new HashJoinMaterializationWorker(leftTable, rightTable, hashKey, indices))
}

class HashJoinMaterializationWorker(leftTable: ActorRef,
                                    rightTable: ActorRef,
                                    hashKey: ValueType,
                                    indices: Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)])
  extends Actor with ActorLogging {

  override def receive: Receive = active(HashJoinMaterializationWorkerState(ActorRef.noSender, None, None, None, None, None, None, Map.empty, Map.empty))

  private def materializeJoinResult(state: HashJoinMaterializationWorkerState): Unit = {
    val newState = state.storeOriginalSender(sender())
    context.become(active(newState))

    leftTable ! ListColumnsInTable()
    rightTable ! ListColumnsInTable()

    leftTable ! GetPartitions()
    rightTable ! GetPartitions()
  }

  private def initiateMaterialization(state: HashJoinMaterializationWorkerState): Unit = {
    log.debug("Initiate Materialization")
    val leftGroupedByPartition = indices.map(_._1).groupBy(_.partitionId)
    val leftRowIdsPerPartition = leftGroupedByPartition.mapValues(_.map(_.rowId).distinct).map(identity)
    val rightGroupedByPartition = indices.map(_._2).groupBy(_.partitionId)
    val rightRowIdsPerPartition = rightGroupedByPartition.mapValues(_.map(_.rowId).distinct).map(identity)

    val newState = state.storeQueriedPartitionCounts(leftGroupedByPartition.size, rightGroupedByPartition.size)
    context.become(active(newState))

    leftRowIdsPerPartition.foreach { case (partitionId, rowIds) => state.leftPartitions.get.apply(partitionId) ! ScanColumns(rowIds)}
    rightRowIdsPerPartition.foreach { case (partitionId, rowIds) => state.rightPartitions.get.apply(partitionId) ! ScanColumns(rowIds)}
  }

  private def handleColumnList(state: HashJoinMaterializationWorkerState, columnNames: Seq[String]): Unit = {
    log.debug("HashJoinMaterializeWorker: handleColumnList")
    val newState = if (sender() == leftTable) {
      state.storeLeftColumnNames(columnNames)
    } else if (sender() == rightTable) {
      state.storeRightColumnNames(columnNames)
    } else {
      throw new Exception("Boooom")
    }

    context.become(active(newState))

    if (newState.hasReceivedColumnsAndPartitions) {
      initiateMaterialization(newState)
    }
  }

  private def handlePartitionsInTable(state: HashJoinMaterializationWorkerState, partitions: Map[Int, ActorRef]): Unit = {
    log.debug("HashJoinMaterializeWorker: handlePartitionsInTable")
    val newState = if (sender() == leftTable) {
      state.storeLeftPartitions(partitions)
    } else if (sender() == rightTable) {
      state.storeRightPartitions(partitions)
    } else {
      throw new Exception("Boooom")
    }

    context.become(active(newState))

    if (newState.hasReceivedColumnsAndPartitions) {
      initiateMaterialization(newState)
    }
  }

  private def handleScannedColumns(state: HashJoinMaterializationWorkerState, partitionId: Int, columns: Map[String, OptionalColumnType]): Unit = {
    log.debug("handle scanned columns")
    val partitionSender = sender() // the partition, we use it to determine from which side it was sent
    val newState = state.storeScannedColumns(partitionSender, partitionId, columns)
    context.become(active(newState))

    if (newState.receivedAllColumns) {
      val leftIndices = indices.map(_._1)
      val rightIndices = indices.map(_._2)

      def iterateIndices(columnName: String,
                         indices: Seq[PartitionedHashTableEntry],
                         values: Map[Int, Map[String, OptionalColumnType]]): ColumnType = {
        val columnValues = indices.map {
          case PartitionedHashTableEntry(pId, rowId, _) => values(pId).apply(columnName).values(rowId).get
        }.toIndexedSeq
        ColumnType(columnValues)
      }

      def reconstructColumns(columnNames: Seq[String],
                             indices: Seq[PartitionedHashTableEntry],
                             values: Map[Int, Map[String, OptionalColumnType]]): Map[String, ColumnType] = {
        columnNames.map(columnName => columnName -> iterateIndices(columnName, indices, values)).toMap
      }

      val leftColumns = reconstructColumns(newState.leftColumnNames.get, leftIndices, newState.leftValues)
      val rightColumns = reconstructColumns(newState.rightColumnNames.get, rightIndices, newState.rightValues)

      val partition = context.actorOf(Partition.props(hashKey, leftColumns ++ rightColumns))
      newState.originalSender ! MaterializedJoinResult(hashKey, partition)
    }
  }

  private def active(state: HashJoinMaterializationWorkerState): Receive = {
    case MaterializeJoinResult() => materializeJoinResult(state)
    case ColumnList(columnNames) => handleColumnList(state, columnNames)
    case PartitionsInTable(partitions) => handlePartitionsInTable(state, partitions)
    case ScannedColumns(partitionId, columns) => handleScannedColumns(state, partitionId, columns)
    case m => throw new Exception(s"Message not understood: $m")
  }

}
