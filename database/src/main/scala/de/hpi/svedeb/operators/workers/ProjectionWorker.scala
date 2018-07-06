package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.operators.workers.ProjectionWorker.{ProjectionJob, ProjectionWorkerResult, ProjectionWorkerState}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Partition.{ColumnRetrieved, GetColumn}
import de.hpi.svedeb.table.{ColumnType, Partition}

object ProjectionWorker {
  case class ProjectionJob()
  case class ProjectionWorkerResult(partitionId: Int, resultPartition: ActorRef)

  case class ProjectionWorkerState(sender: ActorRef, result: Map[String, ColumnType]) {
    def storeSender(sender: ActorRef): ProjectionWorkerState = {
      ProjectionWorkerState(sender, result)
    }

    def addPartialResult(columnName: String, values: ColumnType): ProjectionWorkerState = {
      val newResult = result + (columnName -> values)
      ProjectionWorkerState(sender, newResult)
    }

    def isFinished(expectedColumnNames: Seq[String]): Boolean = {
      result.keys.toSeq.sorted == expectedColumnNames.sorted
    }
  }

  def props(partitionId: Int, partition: ActorRef, columnNames: Seq[String]) = Props(new ProjectionWorker(partitionId, partition, columnNames))
}

class ProjectionWorker(partitionId: Int, partition: ActorRef, columnNames: Seq[String]) extends Actor with ActorLogging{
  override def receive: Receive = active(ProjectionWorkerState(ActorRef.noSender, Map.empty))

  private def retrieveColumnsFromPartition(state: ProjectionWorkerState): Unit = {
    val newState = state.storeSender(sender())
    context.become(active(newState))

    columnNames.foreach(columnName => partition ! GetColumn(columnName))
  }

  private def retrieveValuesFromColumn(state: ProjectionWorkerState, column: ActorRef): Unit = {
    // TODO: Evaluate whether Column Actors should be copied by value or by reference to the new partition.
    column ! ScanColumn(None)
  }

  // why does it make a difference if I hand the partitionId or not?
  // Shouldn't it always be the same one as given in the constructor?
  private def handleScannedValues(state: ProjectionWorkerState, partitionId: Int, columnName: String, values: ColumnType): Unit = {
    val newState = state.addPartialResult(columnName, values)
    context.become(active(newState))

    if (newState.isFinished(columnNames)) {
      val newPartition = context.actorOf(Partition.props(partitionId, newState.result))
      newState.sender ! ProjectionWorkerResult(partitionId, newPartition)
    }
  }

  private def active(state: ProjectionWorkerState): Receive = {
    case ProjectionJob() => retrieveColumnsFromPartition(state)
    case ColumnRetrieved(_, _, column) => retrieveValuesFromColumn(state, column)
    case ScannedValues(pId, columnName, values) => handleScannedValues(state, pId, columnName, values)
  }
}
