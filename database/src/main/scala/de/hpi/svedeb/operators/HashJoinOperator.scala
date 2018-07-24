package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.Execute
import de.hpi.svedeb.operators.HashJoinOperator.HashJoinState
import de.hpi.svedeb.operators.workers.HashWorker
import de.hpi.svedeb.operators.workers.HashWorker.HashJob
import de.hpi.svedeb.utils.Utils.ValueType

object HashJoinOperator {
  private case class HashJoinState()

  def props(leftTable: ActorRef, rightTable: ActorRef, leftJoinColumn: String,
            rightJoinColumn: String,
            predicate: (ValueType, ValueType) => Boolean): Props =
    Props(new HashJoinOperator(leftTable, rightTable, leftJoinColumn, rightJoinColumn, predicate))
}
class HashJoinOperator(leftTable: ActorRef,
                       rightTable: ActorRef,
                       leftJoinColumn: String,
                       rightJoinColumn: String,
                       predicate: (ValueType, ValueType) => Boolean) extends AbstractOperator {
  override def receive: Receive = active(HashJoinState())

  private def initializeJoin(state: HashJoinState): Unit = {
    val leftHashWorker = context.actorOf(HashWorker.props(leftTable, leftJoinColumn))
    val rightHashWorker = context.actorOf(HashWorker.props(rightTable, rightJoinColumn))
    leftHashWorker ! HashJob()
    rightHashWorker ! HashJob()
  }

  private def active(state: HashJoinState): Receive = {
    case Execute() => initializeJoin(state)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
