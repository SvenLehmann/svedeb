package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.helper.HashBucket.{ListValues, ListedValues}
import de.hpi.svedeb.operators.helper.HashBucketEntry
import de.hpi.svedeb.operators.workers.ProbeWorker._
import de.hpi.svedeb.utils.Utils.ValueType

object ProbeWorker {

  case class ProbeJob()
  case class FetchIndices()

  case class ProbeResult(hashKey: Int)
  case class JoinedIndices(joinedIndices: Seq[(HashBucketEntry, HashBucketEntry)])

  case class ProbeWorkerState(originalSender: ActorRef,
                              values: Map[ActorRef, Seq[HashBucketEntry]],
                              joinedIndices: Seq[(HashBucketEntry, HashBucketEntry)]) {
    def storeValues(hashMap: ActorRef, value: Seq[HashBucketEntry]): ProbeWorkerState = {
      ProbeWorkerState(originalSender, values + (hashMap -> value), joinedIndices)
    }

    def storeOriginalSender(sender: ActorRef): ProbeWorkerState = {
      ProbeWorkerState(sender, values, joinedIndices)
    }

    def storeJoinedIndices(joinedIndices: Seq[(HashBucketEntry, HashBucketEntry)]): ProbeWorkerState = {
      ProbeWorkerState(originalSender, values, joinedIndices)
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
  override def receive: Receive = active(ProbeWorkerState(ActorRef.noSender, Map.empty, Seq.empty))

  private def fetchHashMaps(state: ProbeWorkerState): Unit = {
    log.debug("Fetching hashMaps")
    val newState = state.storeOriginalSender(sender())
    context.become(active(newState))

    leftHashMap ! ListValues()
    rightHashMap ! ListValues()
  }

  private def handleListedValues(state: ProbeWorkerState, sendingHashMap: ActorRef, values: Seq[HashBucketEntry]): Unit = {
    log.debug("Handling values from hashMaps")
    val newState = state.storeValues(sendingHashMap, values)
    context.become(active(newState))

    if (newState.receivedAllValues) {
      val leftValues = newState.values(leftHashMap)
      val rightValues = newState.values(rightHashMap)

      log.debug(s"Joining values for key $hash: ${leftValues.size} x ${rightValues.size}")

      // Extracted this as a function for easier performance measurements
      def join(): Seq[(HashBucketEntry, HashBucketEntry)] = {
        for {
        left <- leftValues
        right <- rightValues
        if predicate(left.value, right.value)
        } yield (left, right)
      }

//      val joinedIndices = Utils.time("Time for actual Join", join())
      val joinedIndices = join()
      log.debug(s"Join results ${joinedIndices.size}")
      val newerState = newState.storeJoinedIndices(joinedIndices)
      context.become(active(newerState))
      newState.originalSender ! ProbeResult(hash)
    }
  }

  private def active(state: ProbeWorkerState): Receive = {
    case ProbeJob() => fetchHashMaps(state)
    case ListedValues(values) => handleListedValues(state, sender(), values)
    case FetchIndices() => sender() ! JoinedIndices(state.joinedIndices)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
