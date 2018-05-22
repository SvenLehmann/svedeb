package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.ScanOperator.ScanState
import de.hpi.svedeb.operators.workers.ScanWorker
import de.hpi.svedeb.operators.workers.ScanWorker.{ScanJob, ScanWorkerResult}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table._
import de.hpi.svedeb.utils.Utils

object ScanOperator {
  def props(table: ActorRef, columnName: String, predicate: String => Boolean): Props = Props(new ScanOperator(table, columnName, predicate))

  private case class ScanState(sender: ActorRef,
                               columnName: String,
                               predicate: String => Boolean,
                               numberOfPartitions: Int,
                               columnNames: Option[Seq[String]] = None,
                               results: Map[Int, ActorRef] = Map.empty) {
    def addResult(partitionId: Int, partition: ActorRef): ScanState = {
      val newResults = results + (partitionId -> partition)
      ScanState(sender, columnName, predicate, numberOfPartitions, columnNames, newResults)
    }

    def hasFinished: Boolean = {
      results.keys.size == numberOfPartitions
    }
  }

}

class ScanOperator(table: ActorRef, columnName: String, predicate: String => Boolean) extends AbstractOperator {
  override def receive: Receive = active(ScanState(ActorRef.noSender, null, null, 0))

  private def initializeScan(state: ScanState, columnName: String, predicate: String => Boolean): Unit = {
    val newState = ScanState(sender(), columnName, predicate, 0)
    context.become(active(newState))

    log.debug("Fetching partitions and column names")
    table ! GetPartitions()
    table ! ListColumnsInTable()
  }

  private def invokeScanJobs(state: ScanState, partitions: Seq[ActorRef]): Unit = {
    log.debug("Invoking scan workers")
    // TODO consider using router instead
    val newState = ScanState(state.sender, state.columnName, state.predicate, partitions.size, state.columnNames, state.results)
    context.become(active(newState))

    partitions.zipWithIndex.map{ case (partition, index) => context.actorOf(ScanWorker.props(partition, index, state.columnName, state.predicate)) }.foreach(worker => worker ! ScanJob())
  }

  private def storeColumnNames(state: ScanState, columnNames: Seq[String]): Unit = {
    log.debug("Storing column names")

    val newState = ScanState(state.sender, state.columnName, state.predicate, state.numberOfPartitions, Some(columnNames), state.results)
    context.become(active(newState))

    if (newState.results.size == newState.numberOfPartitions && state.columnNames.isDefined) {
      createNewTable(newState)
    }
  }

  private def storePartialResult(state: ScanState, partitionId: Int, partition: ActorRef): Unit = {
    log.debug("Storing partial result")

    val newState = state.addResult(partitionId, partition)
    context.become(active(newState))

    if (newState.hasFinished && state.columnNames.isDefined) {
      log.debug("Received all partial results.")
      // We received all results for the columns
      createNewTable(newState)
    }
  }

  private def createNewTable(state: ScanState): Unit = {
    val table = context.actorOf(Table.props(
      state.columnNames.get, Utils.defaultPartitionSize, state.results.toSeq.sortBy(_._1).map(_._2)))
    log.debug("Created output table, sending to {}", state.sender)
    state.sender ! QueryResult(table)
  }

  private def active(state: ScanState): Receive = {
    case Execute() => initializeScan(state, columnName, predicate)
    case ColumnList(columnNames) => storeColumnNames(state, columnNames)
    case PartitionsInTable(partitions) => invokeScanJobs(state, partitions)
    case ScanWorkerResult(partitionId, partition) => storePartialResult(state, partitionId, partition)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
