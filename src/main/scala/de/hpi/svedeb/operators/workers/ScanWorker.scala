package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.DataType
import de.hpi.svedeb.operators.workers.ScanWorker.{ScanJob, ScanWorkerResult, State}
import de.hpi.svedeb.table.Column.{FilterColumn, FilteredRowIndices, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.table.{ColumnType, Partition}

object ScanWorker {
  case class ScanJob()

  case class ScanWorkerResult(partitionId:Int, partiton: ActorRef)

  private case class State(sender: Option[ActorRef],
                   columnRefs: Option[Map[String, ActorRef]],
                   result: Map[String, ColumnType[_]] = Map.empty) {
    def addResultForColumn(columnName: String, values: ColumnType[_]): State = {
      val newResultMap = result + (columnName -> values)
      State(sender, columnRefs, newResultMap)
    }

    def storeSender(sender: ActorRef): State = {
      State(Some(sender), columnRefs, result)
    }
  }

  def props(partition: ActorRef,
            partitionId: Int,
            scanColumn: String,
            predicate: String => Boolean): Props = Props(new ScanWorker(partition, partitionId, scanColumn, predicate))
}

class ScanWorker(partition: ActorRef,
                 partitionId: Int,
                 scanColumn: String,
                 predicate: String => Boolean) extends Actor with ActorLogging {

  override def receive: Receive = active(State(None, None))

  private def beginScanJob(state: State): Unit = {
    val newState = state.storeSender(sender())
    context.become(active(newState))
    log.debug(s"Executing Scan job for partition $partitionId, column $scanColumn.")

    partition ! GetColumns()
  }

  private def filterColumn(state: State, columnRefs: Map[String, ActorRef]): Unit = {
    log.debug("Executing filter.")
    val newState = State(state.sender, Some(columnRefs), state.result)
    context.become(active(newState))

    import de.hpi.svedeb.DataTypeImplicits._
    columnRefs(scanColumn) ! FilterColumn(predicate)
  }

  private def scanColumns(state: State, indices: Seq[Int]): Unit = {
    log.debug(s"Scanning columns by indices.")
    state.columnRefs.get.foreach { case (_, columnRef) => columnRef ! ScanColumn(Some(indices)) }
  }

  private def storePartialResult(state: State, columnName: String, values: ColumnType[_]): Unit = {
    log.debug(s"Storing partial result for column $columnName.")
    val newState = state.addResultForColumn(columnName, values)
    context.become(active(newState))

    if (newState.result.size == newState.columnRefs.get.size) {
      log.debug("Received all partial results.")
      // We received all results for the columns
      val partition = context.actorOf(Partition.props(partitionId, newState.result, 10))
      newState.sender.get ! ScanWorkerResult(partitionId, partition)
    }
  }

  private def active(state: State): Receive = {
    case ScanJob() => beginScanJob(state)
    case ColumnsRetrieved(columnRefs) => filterColumn(state, columnRefs)
    case FilteredRowIndices(_, columnName, indices) => scanColumns(state, indices)
    case ScannedValues(_, columnName, values) => storePartialResult(state, columnName, values)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
