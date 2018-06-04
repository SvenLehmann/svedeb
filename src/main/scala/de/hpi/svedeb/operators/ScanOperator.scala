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
  def props(table: ActorRef,
            columnName: String,
            predicate: String => Boolean): Props = Props(new ScanOperator(table, columnName, predicate))

  private case class ScanState(sender: ActorRef,
                               columnName: String,
                               predicate: String => Boolean,
                               numberOfPartitions: Option[Int],
                               columnNames: Option[Seq[String]],
                               results: Map[Int, Option[ActorRef]]) {
    def addResult(partitionId: Int, partition: Option[ActorRef]): ScanState = {
      val newResults = results + (partitionId -> partition)
      ScanState(sender, columnName, predicate, numberOfPartitions, columnNames, newResults)
    }

    def hasFinished: Boolean = {
      numberOfPartitions.isDefined &&
        results.keys.size == numberOfPartitions.get &&
        columnNames.isDefined
    }

    def initializeScan(sender: ActorRef, columnName: String, predicate: String => Boolean): ScanState = {
      ScanState(sender, columnName, predicate, numberOfPartitions, columnNames, results)
    }

    def storePartitionCount(partitionCount: Int): ScanState = {
      ScanState(sender, columnName, predicate, Some(partitionCount), columnNames, results)
    }

    def storeColumnNames(columnNames: Seq[String]): ScanState = {
      ScanState(sender, columnName, predicate, numberOfPartitions, Some(columnNames), results)
    }
  }
}

class ScanOperator(table: ActorRef, columnName: String, predicate: String => Boolean) extends AbstractOperator {
  override def receive: Receive = active(ScanState(ActorRef.noSender, null, null, None, None, Map.empty))

  private def initializeScan(state: ScanState, columnName: String, predicate: String => Boolean): Unit = {
    val newState = state.initializeScan(sender(), columnName, predicate)
    context.become(active(newState))

    log.debug("Fetching partitions and column names")
    table ! GetPartitions()
    table ! ListColumnsInTable()
  }

  private def invokeScanJobs(state: ScanState, partitions: Seq[ActorRef]): Unit = {
    log.debug("Invoking scan workers")
    // TODO consider using router instead
    val newState = state.storePartitionCount(partitions.size)
    context.become(active(newState))

    partitions.zipWithIndex
      .map { case (partition, index) =>
        context.actorOf(ScanWorker.props(partition, index, state.columnName, state.predicate))
      }.foreach(worker => worker ! ScanJob())
  }

  private def storeColumnNames(state: ScanState, columnNames: Seq[String]): Unit = {
    log.debug("Storing column names")
    val newState = state.storeColumnNames(columnNames)
    context.become(active(newState))

    if (newState.hasFinished) {
      createNewTable(newState)
    }
  }

  private def storePartialResult(state: ScanState, partitionId: Int, partition: Option[ActorRef]): Unit = {
    log.debug("Storing partial result")
    val newState = state.addResult(partitionId, partition)
    context.become(active(newState))

    if (newState.hasFinished) {
      log.debug("Received all partial results.")
      createNewTable(newState)
    }
  }

  private def createNewTable(state: ScanState): Unit = {
    val table = context.actorOf(Table.props(
      state.columnNames.get, Utils.defaultPartitionSize, state.results.toSeq.sortBy(_._1).flatMap(_._2)))
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
