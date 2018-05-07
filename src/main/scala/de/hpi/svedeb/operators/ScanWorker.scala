package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.ScanWorker.{ScanJob, ScanJobState, ScanWorkerResult}
import de.hpi.svedeb.table.Column.{FilterColumn, FilteredRowIndizes, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.{ColumnType, Partition}
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}

object ScanWorker {
  case class ScanJob(columnName: String, predicate: String => Boolean)

  case class ScanWorkerResult(partiton: ActorRef)

  case class ScanJobState(sender: Option[ActorRef],
                          columnName: Option[String],
                          columnRefs: Option[Map[String, ActorRef]],
                          predicate: Option[String => Boolean],
                          result: Map[String, ColumnType] = Map.empty[String, ColumnType]) {
    def addResultForColumn(columnName: String, values: ColumnType): ScanJobState = {
      val newResultMap = result + (columnName -> values)
      ScanJobState(sender, this.columnName, columnRefs, predicate, newResultMap)
    }
  }

  def props(partition: ActorRef): Props = Props(new ScanWorker(partition))
}

class ScanWorker(partition: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = active(ScanJobState(None, None, None, None))

  private def beginScanJob(state: ScanJobState, columnName: String, predicate: String => Boolean): Unit = {
    val newState = ScanJobState(Some(sender()), Some(columnName), None, Some(predicate), state.result)
    context.become(active(newState))
    log.info("Executing Scan job.")

    partition ! GetColumns()
  }

  private def filterColumn(state: ScanJobState, columnRefs: Map[String, ActorRef]): Unit = {
    log.info("Executing filter job.")
    val newState = ScanJobState(state.sender, state.columnName, Some(columnRefs), state.predicate, state.result)
    context.become(active(newState))

    columnRefs(state.columnName.get) ! FilterColumn(state.predicate.get)
  }

  def scanColumns(state: ScanJobState, indizes: Seq[Int]): Unit = {
    log.info("Scanning columns.")
    state.columnRefs.get.foreach { case (_, columnRef) => columnRef ! ScanColumn(Some(indizes)) }
  }

  def storePartialResult(state: ScanJobState, columnName: String, values: ColumnType): Unit = {
    log.info("Storing partial result.")
    val newState = state.addResultForColumn(columnName, values)
    context.become(active(newState))

    if (newState.result.size == newState.columnRefs.size) {
      log.info("Received all partial results.")
      // We received all results for the columns
      val partition = context.actorOf(Partition.props(newState.result, 10))
      newState.sender.get ! ScanWorkerResult(partition)
    }
  }

  private def active(state: ScanJobState): Receive = {
    case ScanJob(columnName, predicate) => beginScanJob(state, columnName, predicate)
    case ColumnsRetrieved(columnRefs) => filterColumn(state, columnRefs)
    case FilteredRowIndizes(indizes) => scanColumns(state, indizes)
    case ScannedValues(columnName, values) => storePartialResult(state, columnName, values)
  }
}
