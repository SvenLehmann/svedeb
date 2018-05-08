package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperatorWorker.QueryResult
import de.hpi.svedeb.operators.ScanOperator.{Scan, ScanState}
import de.hpi.svedeb.operators.ScanWorker.{ScanJob, ScanWorkerResult}
import de.hpi.svedeb.table.Partition.{ColumnNameList, ListColumnNames}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table._

object ScanOperator {
  case class Scan(tableName: String, columnName: String, predicate: String => Boolean)

  def props(tableManager: ActorRef): Props = Props(new ScanOperator(tableManager))

  case class ScanState(sender: ActorRef,
                       table: ActorRef,
                       columnName: String,
                       predicate: String => Boolean,
                       numberOfPartitions: Int,
                       columnNames: Option[Seq[String]] = None,
                       results: Seq[ActorRef] = Seq.empty[ActorRef]) {
    def addResult(partition: ActorRef): ScanState = {
      val newResults = results :+ partition
      ScanState(sender, table, columnName, predicate, numberOfPartitions, columnNames, newResults)
    }
  }
}

/*
 * 1. Get Table Actor
 * 2. Get Columns
 * 3. Scan
 * 4. Build result
 */
class ScanOperator(tableManager: ActorRef) extends AbstractOperatorWorker(tableManager) {
  override def receive: Receive = active(ScanState(null, null, null, null, 0))

  private def scan(state: ScanState, table: String, columnName: String, predicate: String => Boolean): Unit = {
    val newState = ScanState(sender(), null, columnName, predicate, 0)
    context.become(active(newState))

    tableManager ! FetchTable(table)
  }

  private def fetchPartitions(state: ScanOperator.ScanState, tableRef: ActorRef) : Unit = {
    log.debug("Fetching partitions and column names")
    tableRef ! GetPartitions()
    tableRef ! ListColumnsInTable()
  }

  private def runScanJobs(state: ScanOperator.ScanState, partitions: Seq[ActorRef]) : Unit = {
    log.debug("Invoking scan workers")
    // TODO consider using router instead
    val newState = ScanState(state.sender, state.table, state.columnName, state.predicate, partitions.size, state.columnNames, state.results)
    context.become(active(newState))

    partitions.map(partition => context.actorOf(ScanWorker.props(partition))).foreach(worker => worker ! ScanJob(state.columnName, state.predicate))
  }

  private def storeColumnNames(state: ScanOperator.ScanState, columnNames: Seq[String]) : Unit = {
    log.debug("Storing column names")

    val newState = ScanState(state.sender, state.table, state.columnName, state.predicate, state.numberOfPartitions, Some(columnNames), state.results)
    context.become(active(newState))

    if (newState.results.size == newState.numberOfPartitions && state.columnNames.isDefined) {
      createNewTable(newState)
    }
  }

  private def storePartialResult(state: ScanOperator.ScanState, partition: ActorRef) : Unit = {
    log.debug("Storing partial result")

    val newState = state.addResult(partition)
    context.become(active(newState))

    if (newState.results.size == newState.numberOfPartitions && state.columnNames.isDefined) {
      log.debug("Received all partial results.")
      // We received all results for the columns
      createNewTable(newState)
    }
  }

  private def createNewTable(state: ScanOperator.ScanState) : Unit = {
    val table = context.actorOf(Table.props(state.columnNames.get, 10, state.results))
    log.debug("Created output table, sending to {}", state.sender)
    state.sender ! QueryResult(table)
  }

  private def active(state: ScanState): Receive = {
    case Scan(table, columnName, predicate) => scan(state, table, columnName, predicate)
    case TableFetched(tableRef) => fetchPartitions(state, tableRef)
    case ColumnList(columnNames) => storeColumnNames(state, columnNames)
    case PartitionsInTable(partitions) => runScanJobs(state, partitions)
    case ScanWorkerResult(partition) => storePartialResult(state, partition)
  }
}
