package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.HashJoinOperator.JoinSide
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{MaterializeJoinResult, MaterializedJoinResult}
import de.hpi.svedeb.operators.workers.PartitionHashWorker.{FetchValuesForKey, FetchedValues}
import de.hpi.svedeb.operators.workers.ProbeWorker.{ProbeJob, ProbeResult, ProbeWorkerState}
import de.hpi.svedeb.utils.Utils.ValueType

object ProbeWorker {

  case class ProbeJob()

  case class ProbeResult(hashKey: Int, joinedIndices: Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)])

  case class ProbeWorkerState(originalSender: ActorRef, values: Map[ActorRef, Seq[PartitionedHashTableEntry]]) {
    def storeValues(hashMap: ActorRef, value: Seq[PartitionedHashTableEntry]): ProbeWorkerState = {
      ProbeWorkerState(originalSender, values + (hashMap -> value))
    }

    def receivedAllValues: Boolean = values.size == 2
  }

  def props(hash: Int, leftHashMap: ActorRef, rightHashMap: ActorRef, predicate: (ValueType, ValueType) => Boolean): Props =
    Props(new ProbeWorker(hash, leftHashMap, rightHashMap, predicate))
}

class ProbeWorker(hash: Int,
                  leftHashMap: ActorRef,
                  rightHashMap: ActorRef,
                  predicate: (ValueType, ValueType) => Boolean) extends Actor with ActorLogging {
  override def receive: Receive = active(ProbeWorkerState(ActorRef.noSender, Map.empty))

  private def fetchHashMaps(): Unit = {
    leftHashMap ! FetchValuesForKey(hash)
    rightHashMap ! FetchValuesForKey(hash)
  }

  private def handleFetchedValues(state: ProbeWorkerState, sendingHashMap: ActorRef, values: Seq[PartitionedHashTableEntry]): Unit = {
    val newState = state.storeValues(sendingHashMap, values)
    context.become(active(newState))

    if (newState.receivedAllValues) {
      val leftValues = newState.values(leftHashMap)
      val rightValues = newState.values(rightHashMap)

      // Extracted this as a function for easier performance measurements
      def join(): Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)] = {
        for {
        left <- leftValues
        right <- rightValues
        if predicate(left.value, right.value)
        } yield (left, right)
      }

      val joinedIndices = join()
      newState.originalSender ! ProbeResult(hash, joinedIndices)
    }
  }

  private def active(state: ProbeWorkerState): Receive = {
    case ProbeJob() => fetchHashMaps()
    case FetchedValues(values) => handleFetchedValues(state, sender(), values)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
