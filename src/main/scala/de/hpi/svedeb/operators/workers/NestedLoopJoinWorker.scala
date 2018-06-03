package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
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
                                     postJoin: Boolean,
                                     result: Map[String, ColumnType]) {
    def hasFinished: Boolean = {
      leftColumnRefs.get.size + rightColumnRefs.get.size == result.size
    }

    def addResultForColumn(columnName: String, values: ColumnType): JoinWorkerState = {
      val newResultMap = result + (columnName -> values)
      JoinWorkerState(
        sender, leftColumnRefs, rightColumnRefs, leftColumnValues, rightColumnValues, postJoin, newResultMap)
    }

    def storeSender(sender: ActorRef): JoinWorkerState = JoinWorkerState(
        Some(sender), leftColumnRefs, rightColumnRefs, leftColumnValues, rightColumnValues, postJoin, result
    )


    def saveColumnRefs(partition: ActorRef,
                       leftPartition: ActorRef,
                       rightPartition: ActorRef,
                       columns: Map[String, ActorRef]): JoinWorkerState = {
      if (partition == leftPartition) {
        JoinWorkerState(sender, Some(columns), rightColumnRefs, leftColumnValues, rightColumnValues, postJoin, result)
      } else if (partition == rightPartition) {
        JoinWorkerState(sender, leftColumnRefs, Some(columns), leftColumnValues, rightColumnValues, postJoin, result)
      } else {
        throw new Exception(s"Unexpected sender $partition")
      }
    }

    def saveColumnValues(columnRef: ActorRef,
                         leftJoinColumn: ActorRef,
                         rightJoinColumn: ActorRef,
                         column: ColumnType): JoinWorkerState = {
      if (columnRef == leftJoinColumn) {
        JoinWorkerState(sender, leftColumnRefs, rightColumnRefs, Some(column), rightColumnValues, postJoin, result)
      } else if (columnRef == rightJoinColumn) {
        JoinWorkerState(sender, leftColumnRefs, rightColumnRefs, leftColumnValues, Some(column), postJoin, result)
      } else {
        throw new Exception(s"Unexpected sender $columnRef")
      }
    }

    def enterPostJoin(): JoinWorkerState = JoinWorkerState(
      sender, leftColumnRefs, rightColumnRefs, leftColumnValues, rightColumnValues, postJoin = true, result)
  }

  def props(leftPartition: ActorRef,
            rightPartition: ActorRef,
            resultPartitionId: Int,
            leftJoinColumn: String,
            rightJoinColumn: String,
            predicate: (String, String) => Boolean): Props =
    Props(new NestedLoopJoinWorker(
      leftPartition, rightPartition, resultPartitionId, leftJoinColumn, rightJoinColumn, predicate)
    )
}

class NestedLoopJoinWorker(leftPartition: ActorRef,
                           rightPartition: ActorRef,
                           resultPartitionId: Int,
                           leftJoinColumn: String,
                           rightJoinColumn: String,
                           predicate: (String, String) => Boolean) extends Actor with ActorLogging {
  override def receive: Receive = active(JoinWorkerState(None, None, None, None, None, postJoin = false, Map.empty))

  private def beginJoinJob(state: JoinWorkerState): Unit = {
    log.debug("Begin Join Job")
    val newState = state.storeSender(sender())
    context.become(active(newState))

    leftPartition ! GetColumns()
    rightPartition ! GetColumns()
  }

  private def handleColumnsRetrieved(state: JoinWorkerState, columns: Map[String, ActorRef]): Unit = {
    log.debug("Handle columns retrieved")
    val newState = state.saveColumnRefs(sender(), leftPartition, rightPartition, columns)
    context.become(active(newState))

    if(newState.leftColumnRefs.isDefined && newState.rightColumnRefs.isDefined) {
      val leftColumn = newState.leftColumnRefs.get.apply(leftJoinColumn)
      leftColumn ! ScanColumn(None)
      val rightColumn = newState.rightColumnRefs.get.apply(rightJoinColumn)
      rightColumn ! ScanColumn(None)
    }
  }

  private def handleScannedValues(state: JoinWorkerState, columnName: String, values: ColumnType): Unit = {
    log.debug("Handle scanned values")
    if (state.postJoin) {
      storeResult(state, columnName, values)
    } else {
      handleJoinInput(state, values)
    }
  }

  private def storeResult(state: JoinWorkerState, columnName: String, values: ColumnType): Unit = {
    val newState = state.addResultForColumn(columnName, values)
    context.become(active(newState))

    log.debug("Received a result")

    if (newState.hasFinished) {
      log.debug("Computed final result")
      val newPartition = context.actorOf(Partition.props(resultPartitionId, newState.result, Utils.defaultPartitionSize))
      newState.sender.get ! PartialResult(resultPartitionId, Some(newPartition))
    }
  }

  private def handleJoinInput(state: JoinWorkerState, values: ColumnType): Unit = {
    val newState = state.saveColumnValues(
      sender(),
      state.leftColumnRefs.get.apply(leftJoinColumn),
      state.rightColumnRefs.get.apply(rightJoinColumn),
      values
    )
    context.become(active(newState))

    log.debug("Received Join input")

    if (newState.leftColumnValues.isDefined && newState.rightColumnValues.isDefined) {
      log.debug("Performing join")
      // using for-comprehension to compute cross product of all potential join combinations
      // Filtering by predicate
      val joinedIndices = for {
        (leftValue, leftIndex) <- newState.leftColumnValues.get.values.zipWithIndex
        (rightValue, rightIndex) <- newState.rightColumnValues.get.values.zipWithIndex
        if predicate(leftValue, rightValue)
      } yield (leftIndex, rightIndex)

      if (joinedIndices.isEmpty) {
        // no need to fetch values again, simply return empty PartialResult
        newState.sender.get ! PartialResult(resultPartitionId, None)
      } else {
        newState.leftColumnRefs.get.values.foreach(column => column ! ScanColumn(Some(joinedIndices.map(_._1))))
        newState.rightColumnRefs.get.values.foreach(column => column ! ScanColumn(Some(joinedIndices.map(_._2))))

        val postJoinState = state.enterPostJoin()
        context.become(active(postJoinState))
      }
    }
  }

  private def active(state: JoinWorkerState): Receive = {
    case JoinJob() => beginJoinJob(state)
    case ColumnsRetrieved(columns) => handleColumnsRetrieved(state, columns)
    case ScannedValues(_, columnName, values) => handleScannedValues(state, columnName, values)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
