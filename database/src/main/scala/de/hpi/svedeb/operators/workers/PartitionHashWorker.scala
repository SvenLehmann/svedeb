package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.HashBucketEntry
import de.hpi.svedeb.operators.workers.PartitionHashWorker._
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnRetrieved, GetColumn}

object PartitionHashWorker {
  case class HashPartition()
  case class HashedPartitionKeys(hashKeys: Seq[Int])
  case class FetchValuesForKey(key: Int)
  case class FetchedValues(key: Int, values: Seq[HashBucketEntry])

  private case class PartitionHashWorkerState(
                                               partitionId: Option[Int],
                                               originalSender: ActorRef,
                                               hashedPartition: Map[Int, Seq[HashBucketEntry]]) {
    def storePartitionId(partitionId: Int): PartitionHashWorkerState = {
      PartitionHashWorkerState(Some(partitionId), originalSender, hashedPartition)
    }

    def storeOriginalSender(sender: ActorRef): PartitionHashWorkerState = {
      PartitionHashWorkerState(partitionId, sender, hashedPartition)
    }

    def storeHashedPartition(hashedPartition: Map[Int, Seq[HashBucketEntry]]): PartitionHashWorkerState = {
      PartitionHashWorkerState(partitionId, originalSender, hashedPartition)
    }
  }

  def props(partition: ActorRef, joinColumn: String): Props = Props(new PartitionHashWorker(partition, joinColumn))
}

/**
  * An actor that hashes a single partition as part of the Hash Phase of the Hash Join
  * @param partition the input partition
  * @param joinColumn the column that is supposed to be hashed on
  */
class PartitionHashWorker(partition: ActorRef, joinColumn: String) extends Actor with ActorLogging {
  override def receive: Receive = active(PartitionHashWorkerState(None, ActorRef.noSender, Map.empty))

  private def initializeHash(state: PartitionHashWorkerState): Unit = {
    log.debug("PartitionHashWorker: Initialize hashing")
    context.become(active(state.storeOriginalSender(sender())))
    partition ! GetColumn(joinColumn)
  }

  private def handleColumnRetrieved(state: PartitionHashWorkerState, partitionId: Int, column: ActorRef): Unit = {
    log.debug("PartitionHashWorker: handleColumnRetrieved")
    context.become(active(state.storePartitionId(partitionId)))
    column ! ScanColumn()
  }

  private def handleScannedValues(state: PartitionHashWorkerState, values: ColumnType): Unit = {
    log.debug("PartitionHashWorker: handleScannedValues (aka hashing)")
    val hashedPartition = values.values.zipWithIndex
      .groupBy(columnValue => columnValue._1 % 50) // TODO: better Hash Function?
      .mapValues(valuesWithSameHash => valuesWithSameHash.map {
        case (value, rowId) => HashBucketEntry(state.partitionId.get, rowId, value)
      }).map(identity)

    val newState = state.storeHashedPartition(hashedPartition)
    context.become(active(newState))

    newState.originalSender ! HashedPartitionKeys(hashedPartition.keys.toSeq)
  }


  private def handleFetchValuesForKey(state: PartitionHashWorkerState, key: Int): Unit = {
    log.debug(s"PartitionHashWorker: handleFetchValuesForKey($key)")
    sender() ! FetchedValues(key, state.hashedPartition(key))
  }

  private def active(state: PartitionHashWorkerState): Receive = {
    case HashPartition() => initializeHash(state)
    case ColumnRetrieved(partitionId, _, column) => handleColumnRetrieved(state, partitionId, column)
    case ScannedValues(_, _, values) => handleScannedValues(state, values)
    case FetchValuesForKey(key) => handleFetchValuesForKey(state, key)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
