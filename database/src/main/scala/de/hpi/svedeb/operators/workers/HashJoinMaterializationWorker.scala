package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{HashJoinMaterializationWorkerState, MaterializeJoinResult}
import de.hpi.svedeb.table.Table.{ColumnList, GetPartitions, ListColumnsInTable, PartitionsInTable}
import de.hpi.svedeb.utils.Utils.ValueType

object HashJoinMaterializationWorker {
  case class MaterializeJoinResult()
  case class MaterializedJoinResult(hashKey: ValueType, partition: ActorRef)

  case class HashJoinMaterializationWorkerState()

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

  override def receive: Receive = active(HashJoinMaterializationWorkerState())

  private def materializeJoinResult(): Unit = {
    leftTable ! ListColumnsInTable()
    rightTable ! ListColumnsInTable()

    leftTable ! GetPartitions()
    rightTable ! GetPartitions()

//    newState.leftColumnRefs.get.values.foreach(column => column ! ScanColumn(Some(joinedIndices.map(_._1))))
//    newState.rightColumnRefs.get.values.foreach(column => column ! ScanColumn(Some(joinedIndices.map(_._2))))

//    PartitionedHashTableEntry(partitionId: Int, rowId: Int, value: ValueType)

//    indices.foreach { case (l, r) =>
//      leftTable !
//    }
  }

  private def handleColumnList(state: HashJoinMaterializationWorkerState, columnNames: Seq[String]): Unit = {
    log.debug("HashJoinMaterializeWorker: handleColumnList")
  }

  private def handlePartitionsInTable(state: HashJoinMaterializationWorkerState, partitions: Map[Int, ActorRef]): Unit = {
    log.debug("HashJoinMaterializeWorker: handlePartitionsInTable")
  }

  private def active(state: HashJoinMaterializationWorkerState): Receive = {
    case MaterializeJoinResult() => materializeJoinResult()
    case ColumnList(columnNames) => handleColumnList(state, columnNames)
    case PartitionsInTable(partitions) => handlePartitionsInTable(state, partitions)
    case m => throw new Exception(s"Message not understood: $m")
  }

}
