package de.hpi.svedeb.operators.helper

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor._
import de.hpi.svedeb.operators.workers.PartitionHashWorker.{FetchValuesForKey, FetchedValues}
import de.hpi.svedeb.utils.Utils.ValueType

object PartitionedHashTableActor {
  case class ListValues()
  case class ListedValues(values: Seq[PartitionedHashTableEntry])
  case class FetchValues()
  case class FetchedHashedValues(hashKey: ValueType)

  private case class HashTableState(originalSender: ActorRef, answerCount: Int, values: Seq[PartitionedHashTableEntry]) {
    def storeOriginalSender(sender: ActorRef): HashTableState = {
      HashTableState(sender, answerCount, values)
    }

    def storeIntermediateResult(newValues: Seq[PartitionedHashTableEntry]): HashTableState = {
      HashTableState(originalSender, answerCount + 1, values ++ newValues)
    }

    def isFinished(expectedCount: Int): Boolean = {
      expectedCount == answerCount
    }
  }

  def props(hashKey: ValueType, actorRefs: Seq[ActorRef]): Props = Props(new PartitionedHashTableActor(hashKey, actorRefs))
}

class PartitionedHashTableActor(hashKey: Int, actorRefs: Seq[ActorRef]) extends Actor with ActorLogging {
  override def receive: Receive = active(HashTableState(ActorRef.noSender, 0, Seq.empty))
  private def handleFetchValues(state: HashTableState): Unit = {
    context.become(active(state.storeOriginalSender(sender())))
    actorRefs.foreach(actorRef => actorRef ! FetchValuesForKey(hashKey))
  }

  private def handleFetchedValues(state: HashTableState, values: Seq[PartitionedHashTableEntry]): Unit = {
    val newState = state.storeIntermediateResult(values)
    context.become(active(newState))

    if (newState.isFinished(actorRefs.size)) {
      newState.originalSender ! FetchedHashedValues(hashKey)
    }
  }

  private def handleListValues(state: HashTableState): Unit = {
    sender() ! ListedValues(state.values)
  }

  private def active(state: HashTableState): Receive = {
    case FetchValues() => handleFetchValues(state)
    case FetchedValues(values) => handleFetchedValues(state, values)
    case ListValues() => handleListValues(state)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
