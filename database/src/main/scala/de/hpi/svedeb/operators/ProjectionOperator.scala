package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Deploy, Props}
import akka.remote.RemoteScope
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.ProjectionOperator.ProjectionState
import de.hpi.svedeb.operators.workers.{ProjectionWorker, ScanWorker}
import de.hpi.svedeb.operators.workers.ProjectionWorker.{ProjectionJob, ProjectionWorkerResult}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table.{GetPartitions, PartitionsInTable}



/**
  * TODO: Projection is currently materializing the whole table in its local state.
  *
  * Better: Create ProjectionWorkers for each partition on those nodes that actually hold the partition
  * Evaluate whether Column Actors should be copied by value or by reference to the new partition.
  */
object ProjectionOperator {
  def props(input: ActorRef, columnNames: Seq[String]): Props = Props(new ProjectionOperator(input, columnNames))

  private case class ProjectionState(sender: ActorRef, result: Map[Int, ActorRef], outputPartitionCount: Int) {
    def storeSender(sender: ActorRef): ProjectionState = {
      ProjectionState(sender, result, outputPartitionCount)
    }

    def storeOutputPartitionCount(count: Int): ProjectionState = {
      ProjectionState(sender, result, count)
    }

    def storeResultPartition(partitionId:Int, resultPartition: ActorRef): ProjectionState = {
      ProjectionState(sender, result + (partitionId -> resultPartition), outputPartitionCount)
    }

    def isFinished: Boolean = {
      result.keys.toSeq.length == outputPartitionCount
    }
  }
}

class ProjectionOperator(input: ActorRef, columnNames: Seq[String]) extends AbstractOperator {
  override def receive: Receive = active(ProjectionState(ActorRef.noSender, Map.empty, 0))

  private def handleQuery(state: ProjectionState, sender: ActorRef): Unit = {
    val newState = state.storeSender(sender)
    context.become(active(newState))

    if (columnNames.isEmpty) {
      val emptyTable = context.actorOf(Table.propsWithPartitions(columnNames, Map.empty))
      sender ! QueryResult(emptyTable)
    } else {
      input ! GetPartitions()
    }
  }

  private def handlePartitionsInTable(state: ProjectionState, partitions: Map[Int, ActorRef]): Unit = {
    val newState = state.storeOutputPartitionCount(partitions.toSeq.length)
    context.become(active(newState))

    partitions.foreach{ case (partitionId, partition) =>
      // TODO: Add multi-node test
      val address = partition.path.address
      log.debug(s"Starting ProjectionWorker at $address")
      val worker = context.system.actorOf(
        ProjectionWorker
          .props(partitionId, partition, columnNames)
          .withDeploy(new Deploy(RemoteScope(address))))
      worker ! ProjectionJob()
    }
  }

  private def handleWorkerResult(state: ProjectionState, partitionId: Int, resultPartition: ActorRef): Unit = {
    val newState = state.storeResultPartition(partitionId, resultPartition)
    context.become(active(newState))

    if(newState.isFinished) {
      val resultTable = context.actorOf(Table.propsWithPartitions(columnNames, newState.result))
      newState.sender ! QueryResult(resultTable)
    }
  }

  private def active(state: ProjectionState): Receive = {
    case Execute() => handleQuery(state, sender())
    case PartitionsInTable(partitions) => handlePartitionsInTable(state, partitions)
    case ProjectionWorkerResult(partitionId, resultPartition) => handleWorkerResult(state, partitionId, resultPartition)
  }
}
