package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.workers.ScanWorker.{ScanJob, State, ScanWorkerResult}
import de.hpi.svedeb.table.Column.{FilterColumn, FilteredRowIndizes, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.table.{ColumnType, Partition}

object ScanWorker {
  case class ScanJob(columnName: String, predicate: String => Boolean)

  case class ScanWorkerResult(partitionId:Int, partiton: ActorRef)

  private case class State(sender: Option[ActorRef],
                   columnName: Option[String],
                   columnRefs: Option[Map[String, ActorRef]],
                   predicate: Option[String => Boolean],
                   result: Map[String, ColumnType] = Map.empty[String, ColumnType]) {
    def addResultForColumn(columnName: String, values: ColumnType): State = {
      val newResultMap = result + (columnName -> values)
      State(sender, this.columnName, columnRefs, predicate, newResultMap)
    }
  }

  def props(partition: ActorRef, partitionId: Int): Props = Props(new ScanWorker(partition, partitionId))
}

class ScanWorker(partition: ActorRef, partitionId: Int) extends Actor with ActorLogging {

  override def receive: Receive = active(State(None, None, None, None))

  private def beginScanJob(state: State, columnName: String, predicate: String => Boolean): Unit = {
    val newState = State(Some(sender()), Some(columnName), None, Some(predicate), state.result)
    context.become(active(newState))
    log.debug("Executing Scan job.")

    partition ! GetColumns()
  }

  private def filterColumn(state: State, columnRefs: Map[String, ActorRef]): Unit = {
    log.debug("Executing filter job.")
    val newState = State(state.sender, state.columnName, Some(columnRefs), state.predicate, state.result)
    context.become(active(newState))

    columnRefs(state.columnName.get) ! FilterColumn(state.predicate.get)
  }

  private def scanColumns(state: State, indizes: Seq[Int]): Unit = {
    log.debug("Scanning columns.")
    state.columnRefs.get.foreach { case (_, columnRef) => columnRef ! ScanColumn(Some(indizes)) }
  }

  private def storePartialResult(state: State, columnName: String, values: ColumnType): Unit = {
    log.debug("Storing partial result for column {}.", columnName)
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
    case ScanJob(columnName, predicate) => beginScanJob(state, columnName, predicate)
    case ColumnsRetrieved(columnRefs) => filterColumn(state, columnRefs)
    case FilteredRowIndizes(_, columnName, indizes) => scanColumns(state, indizes)
    case ScannedValues(_, columnName, values) => storePartialResult(state, columnName, values)
    case m => throw new Exception("Message not understood: " + m)
  }
}
