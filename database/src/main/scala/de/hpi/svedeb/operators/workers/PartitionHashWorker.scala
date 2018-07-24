package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.Execute
import de.hpi.svedeb.operators.HashJoinOperator.HashJoinState
import de.hpi.svedeb.operators.workers.PartitionHashWorker._
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnRetrieved, GetColumn}
import de.hpi.svedeb.utils.Utils.ValueType
object PartitionHashWorker {
  case class HashPartition()
  case class HashedPartitionKeys(hashKeys: Seq[Int])
  case class FetchValuesForKey(key: Int)
  case class FetchedValues(values: Seq[(Int, Int, ValueType)])

  private case class PartitionHashWorkerState(partitionId: Option[Int], originalSender: ActorRef, hashedPartition: Map[Int, Seq[(Int, Int, ValueType)]]) {
    def storePartitionId(partitionId: Int): PartitionHashWorkerState = {
      PartitionHashWorkerState(Some(partitionId), originalSender, hashedPartition)
    }

    def storeOriginalSender(sender: ActorRef): PartitionHashWorkerState = {
      PartitionHashWorkerState(partitionId, sender, hashedPartition)
    }

    // TODO change hash function
    def storeHashedPartition(hashedPartition: Map[Int, Seq[(Int, Int, ValueType)]]): PartitionHashWorkerState = {
      PartitionHashWorkerState(partitionId, originalSender, hashedPartition)
    }
  }

  def props(partition: ActorRef, joinColumn: String): Props = Props(new PartitionHashWorker(partition, joinColumn))
}

class PartitionHashWorker(partition: ActorRef, joinColumn: String) extends Actor with ActorLogging {
  override def receive: Receive = active(PartitionHashWorkerState(None, ActorRef.noSender, Map.empty))

  private def initializeHash(state: PartitionHashWorkerState): Unit = {
    context.become(active(state.storeOriginalSender(sender())))
    partition ! GetColumn(joinColumn)
  }

  private def handleColumnRetrieved(state: PartitionHashWorkerState, partitionId: Int, column: ActorRef): Unit = {
    context.become(active(state.storePartitionId(partitionId)))
    column ! ScanColumn()
  }

  private def handleScannedValues(state: PartitionHashWorkerState, values: ColumnType): Unit = {
    val hashedPartition = values.values.zipWithIndex
      .groupBy(columnValue => columnValue._1) // TODO Change hash function
      .mapValues(valuesWithSameHash => valuesWithSameHash.map {
        case (value, rowId) => (state.partitionId.get, rowId, value)
      })

    val newState = state.storeHashedPartition(hashedPartition)
    context.become(active(newState))

    newState.originalSender ! HashedPartitionKeys(hashedPartition.keys.toSeq)
  }

  private def handleFetchValuesForKey(state: PartitionHashWorkerState, key: Int): Unit = {
    sender() ! FetchedValues(state.hashedPartition(key))
  }

  private def active(state: PartitionHashWorkerState): Receive = {
    case HashPartition() => initializeHash(state)
    case ColumnRetrieved(partitionId, _, column) => handleColumnRetrieved(state, partitionId, column)
    case ScannedValues(_, _, values) => handleScannedValues(state, values)
    case FetchValuesForKey(key) => handleFetchValuesForKey(state, key)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
