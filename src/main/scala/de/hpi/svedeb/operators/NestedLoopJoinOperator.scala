package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.Execute
import de.hpi.svedeb.operators.NestedLoopJoinOperator.JoinState
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker.PartialResult
import de.hpi.svedeb.table.Table.PartitionsInTable

object NestedLoopJoinOperator {
  def props(leftTable: ActorRef, rightTable: ActorRef): Props = Props(new NestedLoopJoinOperator(leftTable, rightTable))

  private case class JoinState(originalSender: ActorRef)
}

class NestedLoopJoinOperator(leftTable: ActorRef, rightTable: ActorRef) extends AbstractOperator {
  override def receive: Receive = active(JoinState(ActorRef.noSender))

  def initializeJoin(): Unit = {
    // fetch partitions
    context.become(active(JoinState(sender())))
  }

  def handlePartitions(partitions: Seq[ActorRef]): Unit = {
    // save once with sender
    // invoke join jobs else
    // save number of jobs, forward partition id
  }

  def handlePartialResult(partitionId: Int, partition: Option[ActorRef]): Unit = {
    // store result
    // if all received, create new table and return
  }

  private def active(state: JoinState): Receive = {
    case Execute() => initializeJoin()
    case PartitionsInTable(partitions) => handlePartitions(partitions)  // get partitions
    case PartialResult(partitionId, partition) => handlePartialResult(partitionId, partition)
    case m => throw new Exception(s"Message not understood: $m")
  }
}

