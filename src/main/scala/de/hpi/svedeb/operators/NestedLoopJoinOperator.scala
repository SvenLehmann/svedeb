package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.NestedLoopJoinOperator.JoinState
import de.hpi.svedeb.operators.ScanOperator.ScanState
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker.{JoinJob, PartialResult}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table.{ColumnList, GetPartitions, ListColumnsInTable, PartitionsInTable}
import de.hpi.svedeb.utils.Utils

object NestedLoopJoinOperator {
  def props(leftTable: ActorRef, rightTable: ActorRef, leftJoinColumn: String,
            rightJoinColumn: String,
            predicate: (String, String) => Boolean): Props = Props(new NestedLoopJoinOperator(leftTable, rightTable, leftJoinColumn, rightJoinColumn, predicate))

  private case class JoinState(originalSender: ActorRef,
                               leftPartitions: Option[Seq[ActorRef]],
                               rightPartitions: Option[Seq[ActorRef]],
                               leftColumnNames: Option[Seq[String]],
                               rightColumnNames: Option[Seq[String]],
                               result: Map[Int, Option[ActorRef]]) {
    def storeSender(sender: ActorRef): JoinState = {
      JoinState(sender, leftPartitions, rightPartitions, leftColumnNames, rightColumnNames, result)
    }

    def storePartitions(sender: ActorRef, leftTable: ActorRef, rightTable: ActorRef, partitions: Seq[ActorRef]): JoinState = {
      if (sender == leftTable) {
        JoinState(originalSender, Some(partitions), rightPartitions, leftColumnNames, rightColumnNames, result)
      } else if (sender == rightTable) {
        JoinState(originalSender, leftPartitions, Some(partitions), leftColumnNames, rightColumnNames, result)
      } else {
        throw new Exception(s"Unexpected sender $sender")
      }
    }

    def storePartialResult(partitionId: Int, partition: Option[ActorRef]): JoinState = {
      JoinState(originalSender, leftPartitions, rightPartitions, leftColumnNames, rightColumnNames, result + (partitionId -> partition))
    }

    def storeColumnNames(sender: ActorRef, leftTable: ActorRef, rightTable: ActorRef, columnNames: Seq[String]): JoinState = {
      if (sender == leftTable) {
        JoinState(originalSender, leftPartitions, rightPartitions, Some(columnNames), rightColumnNames, result)
      } else if (sender == rightTable) {
        JoinState(originalSender, leftPartitions, rightPartitions, leftColumnNames, Some(columnNames), result)
      } else {
        throw new Exception(s"Unexpected sender $sender")
      }
    }

    def hasFinished: Boolean = {
      leftPartitions.isDefined &&
        rightPartitions.isDefined &&
        result.size == leftPartitions.get.size * rightPartitions.get.size &&
        leftColumnNames.isDefined &&
        rightColumnNames.isDefined
    }
  }
}

class NestedLoopJoinOperator(leftTable: ActorRef,
                             rightTable: ActorRef,
                             leftJoinColumn: String,
                             rightJoinColumn: String,
                             predicate: (String, String) => Boolean) extends AbstractOperator {
  override def receive: Receive = active(JoinState(ActorRef.noSender, None, None, None, None, Map.empty))

  def initializeJoin(state: JoinState): Unit = {
    leftTable ! GetPartitions()
    rightTable ! GetPartitions()
    leftTable ! ListColumnsInTable()
    rightTable ! ListColumnsInTable()
    context.become(active(state.storeSender(sender())))
  }

  def handlePartitions(state: JoinState, partitions: Seq[ActorRef]): Unit = {
    val newState = state.storePartitions(sender(), leftTable, rightTable, partitions)
    context.become(active(newState))

    if (newState.leftPartitions.isDefined && newState.rightPartitions.isDefined) {
      log.debug("Invoke Join Workers")
      val rightPartitionSize = newState.rightPartitions.get.size

      for {
        (leftPartition, leftIndex) <- newState.leftPartitions.get.zipWithIndex
        (rightPartition, rightIndex) <- newState.rightPartitions.get.zipWithIndex
      } yield {
        val newPartitionId = leftIndex * rightPartitionSize + rightIndex
        log.debug(s"PartitionId for NLJWorker: $newPartitionId")
        val worker = context.actorOf(NestedLoopJoinWorker.props(
          leftPartition, rightPartition, newPartitionId,
          leftJoinColumn, rightJoinColumn, predicate))
        worker ! JoinJob()
      }
    }
  }

  def handleColumnNames(state: JoinState, columnNames: Seq[String]): Unit = {
    val newState = state.storeColumnNames(sender(), leftTable, rightTable, columnNames)
    context.become(active(newState))

    if (newState.hasFinished) {
      createNewTable(newState)
    }
  }

  def handlePartialResult(state: JoinState, partitionId: Int, partition: Option[ActorRef]): Unit = {
    val newState = state.storePartialResult(partitionId, partition)
    context.become(active(newState))

    if (newState.hasFinished) {
      createNewTable(newState)
    }
  }

  private def createNewTable(state: JoinState): Unit = {
    val table = context.actorOf(Table.props(
      state.leftColumnNames.get ++ state.rightColumnNames.get,
      Utils.defaultPartitionSize,
      state.result.toSeq.sortBy(_._1).flatMap(_._2)))
    log.debug("Created output table, sending to {}", state.originalSender)
    state.originalSender ! QueryResult(table)
  }

  private def active(state: JoinState): Receive = {
    case Execute() => initializeJoin(state)
    case PartitionsInTable(partitions) => handlePartitions(state, partitions)
    case ColumnList(columnNames) => handleColumnNames(state, columnNames)
    case PartialResult(partitionId, partition) => handlePartialResult(state, partitionId, partition)
    case m => throw new Exception(s"Message not understood: $m")
  }
}

