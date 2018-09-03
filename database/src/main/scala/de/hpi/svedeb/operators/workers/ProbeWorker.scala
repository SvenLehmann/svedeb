package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor.{ListValues, ListedValues}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.ProbeWorker.{ProbeJob, ProbeResult, ProbeWorkerState}
import de.hpi.svedeb.utils.Utils
import de.hpi.svedeb.utils.Utils.ValueType

object ProbeWorker {

  case class ProbeJob()

  case class ProbeResult(hashKey: Int, joinedIndices: Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)])

  case class ProbeWorkerState(originalSender: ActorRef, values: Map[ActorRef, Seq[PartitionedHashTableEntry]]) {
    def storeValues(hashMap: ActorRef, value: Seq[PartitionedHashTableEntry]): ProbeWorkerState = {
      ProbeWorkerState(originalSender, values + (hashMap -> value))
    }

    def storeOriginalSender(sender: ActorRef): ProbeWorkerState = {
      ProbeWorkerState(sender, values)
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

  private def fetchHashMaps(state: ProbeWorkerState): Unit = {
    log.debug("Fetching hashMaps")
    val newState = state.storeOriginalSender(sender())
    context.become(active(newState))

    leftHashMap ! ListValues()
    rightHashMap ! ListValues()
  }

  private def handleListedValues(state: ProbeWorkerState, sendingHashMap: ActorRef, values: Seq[PartitionedHashTableEntry]): Unit = {
    log.debug("Handling values from hashMaps")
    val newState = state.storeValues(sendingHashMap, values)
    context.become(active(newState))

    if (newState.receivedAllValues) {
      val leftValues = newState.values(leftHashMap)
      val rightValues = newState.values(rightHashMap)

      log.debug(s"Joining values for key $hash: ${leftValues.size} x ${rightValues.size}")

      // Extracted this as a function for easier performance measurements
      def join(): Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)] = {
        for {
        left <- leftValues
        right <- rightValues
        if predicate(left.value, right.value)
        } yield (left, right)
      }

//      val joinedIndices = Utils.time("Time for actual Join", join())
      val joinedIndices = join()
      log.debug(s"Join results ${joinedIndices.size}")
      newState.originalSender ! ProbeResult(hash, joinedIndices)
    }
  }

  private def active(state: ProbeWorkerState): Receive = {
    case ProbeJob() => fetchHashMaps(state)
    case ListedValues(values) => handleListedValues(state, sender(), values)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
