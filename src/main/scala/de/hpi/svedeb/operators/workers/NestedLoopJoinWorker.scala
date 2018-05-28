package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef}
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker.{JoinJob, JoinWorkerState, PartialResult}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.{ColumnType, Partition}
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.utils.Utils

object NestedLoopJoinWorker {
  case class JoinJob()
  case class PartialResult(partitionId: Int, partition: Option[ActorRef])

  // TODO: columns could have same name -- rename?
  private case class JoinWorkerState(sender: Option[ActorRef],
                                     leftColumnRefs: Option[Map[String, ActorRef]],
                                     rightColumnRefs: Option[Map[String, ActorRef]],
                                     leftColumnValues: Option[ColumnType],
                                     rightColumnValues: Option[ColumnType],
                                     postJoin: Boolean = false,
                                     result: Map[String, ColumnType] = Map.empty[String, ColumnType]) {
    def addResultForColumn(columnName: String, values: ColumnType): JoinWorkerState = {
      val newResultMap = result + (columnName -> values)
      JoinWorkerState(sender, leftColumnRefs, rightColumnRefs, leftColumnValues, rightColumnValues, postJoin, newResultMap)
    }

    def storeSender(sender: ActorRef): JoinWorkerState = {
      JoinWorkerState(Some(sender), leftColumnRefs, rightColumnRefs, leftColumnValues, rightColumnValues, postJoin, result)
    }

    def saveColumnRefs(partition: ActorRef, leftPartition: ActorRef, rightPartition: ActorRef, columns: Map[String, ActorRef]): JoinWorkerState = {
      if (partition == leftPartition) JoinWorkerState(sender, Some(columns), rightColumnRefs, leftColumnValues, rightColumnValues, postJoin, result)
      else JoinWorkerState(sender, leftColumnRefs, Some(columns), leftColumnValues, rightColumnValues, postJoin, result)
    }

    def saveColumnValues(partition: ActorRef, leftPartition: ActorRef, rightPartition: ActorRef, column: ColumnType): JoinWorkerState = {
      if (partition == leftPartition) JoinWorkerState(sender, leftColumnRefs, rightColumnRefs, Some(column), rightColumnValues, postJoin, result)
      else JoinWorkerState(sender, leftColumnRefs, rightColumnRefs, leftColumnValues, Some(column), postJoin, result)
    }

    def enterPostJoin(): JoinWorkerState = {
      JoinWorkerState(sender, leftColumnRefs, rightColumnRefs, leftColumnValues, rightColumnValues, postJoin = true, result)
    }
  }
}

class NestedLoopJoinWorker(leftPartition: ActorRef,
                           rightPartition: ActorRef,
                           resultPartitionId: Int,
                           leftJoinColumn: String,
                           rightJoinColumn: String) extends Actor with ActorLogging {
  override def receive: Receive = active(JoinWorkerState(None, None, None, None, None))

  private def beginJoinJob(state: JoinWorkerState): Unit = {
    val newState = state.storeSender(sender())
    context.become(active(newState))

    leftPartition ! GetColumns()
    rightPartition ! GetColumns()
  }

  def handleColumnsRetrieved(state: JoinWorkerState, columns: Map[String, ActorRef]): Unit = {
    val newState = state.saveColumnRefs(sender(), leftPartition, rightPartition, columns)
    context.become(active(newState))

    if(newState.leftColumnRefs.isDefined && newState.rightColumnRefs.isDefined) {
      newState.leftColumnRefs.get.apply(leftJoinColumn) ! ScanColumn(None)
      newState.rightColumnRefs.get.apply(rightJoinColumn) ! ScanColumn(None)
    }
  }

  def handleScannedValues(state: JoinWorkerState, columnName: String, values: ColumnType): Unit = {
    if (state.postJoin) {
      storeResult(state, columnName, values)
    } else {
      handleJoinInput(state, values)
    }
  }

  private def storeResult(state: JoinWorkerState, columnName: String, values: ColumnType): Unit = {
    val newState = state.addResultForColumn(columnName, values)
    context.become(active(newState))

    if (newState.leftColumnRefs.get.size + newState.rightColumnRefs.get.size == newState.result.size) {
      if (newState.result.forall(_._2.values.isEmpty)) {
        newState.sender.get ! PartialResult(resultPartitionId, None)
      } else {
        val newPartition = context.actorOf(Partition.props(resultPartitionId, newState.result, Utils.defaultPartitionSize))
        newState.sender.get ! PartialResult(resultPartitionId, Some(newPartition))
      }
    }
  }

  private def handleJoinInput(state: JoinWorkerState, values: ColumnType): Unit = {
    val newState = state.saveColumnValues(sender(), leftPartition, rightPartition, values)
    context.become(active(newState))

    if (newState.leftColumnValues.isDefined && newState.rightColumnValues.isDefined) {
      val indexedLeft = newState.leftColumnValues.get.values.zipWithIndex
      val indexedRight = newState.rightColumnValues.get.values.zipWithIndex

      val joinedIndices = indexedLeft.flatMap { case (leftValue, leftIndex) =>
        indexedRight.flatMap { case (rightValue, rightIndex) =>
          if (leftValue == rightValue) Some(leftIndex, rightIndex)
          else None
        }
      }

      newState.leftColumnRefs.get.foreach { case (_, column) => column ! ScanColumn(Some(joinedIndices.map(_._1))) }
      newState.rightColumnRefs.get.foreach { case (_, column) => column ! ScanColumn(Some(joinedIndices.map(_._2))) }

      val postJoinState = state.enterPostJoin()
      context.become(active(postJoinState))
    }
  }

  def active(state: JoinWorkerState): Receive = {
    case JoinJob() => beginJoinJob(state)
    case ColumnsRetrieved(columns) => handleColumnsRetrieved(state, columns)
    case ScannedValues(partitionId, columnName, values) => handleScannedValues(state, columnName, values)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
