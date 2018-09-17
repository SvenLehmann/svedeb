package de.hpi.svedeb.operators.helper

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.HashBucket._
import de.hpi.svedeb.operators.workers.PartitionHashWorker.{FetchValuesForKey, FetchedValues}
import de.hpi.svedeb.utils.Utils.ValueType

object HashBucket {
  case class ListValues()
  case class ListedValues(values: Seq[HashBucketEntry])
  case class BuildPartitionedHashTable()
  case class BuiltPartitionedHashTable(hashKey: ValueType)

  private case class HashTableState(originalSender: ActorRef, answerCount: Int, values: Seq[HashBucketEntry]) {
    def storeOriginalSender(sender: ActorRef): HashTableState = {
      HashTableState(sender, answerCount, values)
    }

    def storeIntermediateResult(newValues: Seq[HashBucketEntry]): HashTableState = {
      HashTableState(originalSender, answerCount + 1, values ++ newValues)
    }

    def isFinished(expectedCount: Int): Boolean = {
      expectedCount == answerCount
    }
  }

  def props(hashKey: ValueType, actorRefs: Seq[ActorRef]): Props = Props(new HashBucket(hashKey, actorRefs))
}

/**
  * This class can be seen as one element in a HashMap.
  * HashKey is the key of the HashMap, the value-side is generated when the message BuildPartitionedHashTable() is sent
  * @param hashKey the key
  * @param partitionHashWorkers the actors which hold the data
  */
class HashBucket(hashKey: Int, partitionHashWorkers: Seq[ActorRef]) extends Actor with ActorLogging {
  override def receive: Receive = active(HashTableState(ActorRef.noSender, 0, Seq.empty))
  private def buildHashTable(state: HashTableState): Unit = {
    log.debug("handle fetch values")
    context.become(active(state.storeOriginalSender(sender())))
    partitionHashWorkers.foreach(partitionHashWorker => partitionHashWorker ! FetchValuesForKey(hashKey))
  }

  private def handleFetchedValues(state: HashTableState, key: Int, values: Seq[HashBucketEntry]): Unit = {
    log.debug(s"handle fetched values $key (${values.size} elements)")
    val newState = state.storeIntermediateResult(values)
    context.become(active(newState))

    if (newState.isFinished(partitionHashWorkers.size)) {
      newState.originalSender ! BuiltPartitionedHashTable(hashKey)
    }
  }

  private def handleListValues(state: HashTableState): Unit = {
    log.debug("handle list values")
    sender() ! ListedValues(state.values)
  }

  private def active(state: HashTableState): Receive = {
    case BuildPartitionedHashTable() => buildHashTable(state)
    case FetchedValues(key, values) => handleFetchedValues(state, key, values)
    case ListValues() => handleListValues(state)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
