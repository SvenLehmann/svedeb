package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.CreateTableOperator.CreateTableOperatorState
import de.hpi.svedeb.table.ColumnType

import scala.util.Random

object CreateTableOperator {
  def props(tableManager: ActorRef, tableName: String, data: Map[Int, Map[String, ColumnType]], partitionSize: Int): Props =
    Props(new CreateTableOperator(tableManager, tableName, data, partitionSize))

  private case class CreateTableOperatorState(originalSender: ActorRef,
                                              partitionMapping: Map[Int, ActorRef],
                                              tableAdded: Map[ActorRef, Boolean],
                                              result: ActorRef) {
    def receivedAllRemoteAcknowledgements(): Boolean = {
      tableAdded.forall(_._2)
    }

    def updateResult(tableManager: ActorRef): CreateTableOperatorState = {
      CreateTableOperatorState(originalSender, partitionMapping, tableAdded, tableManager)
    }

    def addRemoteTableAcknowledgement(tableManagerState: ActorRef): CreateTableOperatorState = {
      val updatedTableAdded = tableAdded + (tableManagerState -> true)
      CreateTableOperatorState(originalSender, partitionMapping, updatedTableAdded, result)
    }

    def addSender(sender: ActorRef): CreateTableOperatorState = {
      CreateTableOperatorState(sender, partitionMapping, tableAdded, result)
    }

    def addPartitionMapping(partitionMapping: Map[Int, ActorRef]): CreateTableOperatorState = {
      CreateTableOperatorState(originalSender, partitionMapping, tableAdded, result)
    }

    def addPartition(partitionId: Int, partition: ActorRef): CreateTableOperatorState = {
      val updatedMapping = partitionMapping + (partitionId -> partition)
      CreateTableOperatorState(originalSender, updatedMapping, tableAdded, result)
    }

    def receivedAllPartitions(): Boolean = {
      partitionMapping.forall{ case (_, partition) => partition != ActorRef.noSender }
    }
  }
}

class CreateTableOperator(localTableManager: ActorRef,
                          tableName: String,
                          data: Map[Int, Map[String, ColumnType]],
                          partitionSize: Int) extends AbstractOperator {
  override def receive: Receive = active(CreateTableOperatorState(ActorRef.noSender, Map.empty, Map.empty, ActorRef.noSender))

  private def getAllTableManagers: Seq[ActorRef] = {
    Seq(localTableManager)
  }

  private def execute(state: CreateTableOperatorState): Unit = {
    log.debug("Execute")
    val addedSenderState = state.addSender(sender())
    context.become(active(addedSenderState))

    val allTableManagers: Seq[ActorRef] = getAllTableManagers
    val partitionManagerMappings = data.map { case (partitionId, values) =>
      val random = new Random()
      val chosenTableManager = allTableManagers(random.nextInt(allTableManagers.length))
      (partitionId, values, chosenTableManager)
    }

    val emptyPartitionMap = partitionManagerMappings.map{ case (partitionId, _, _) => partitionId -> ActorRef.noSender}.toMap
    val partitionMappingState = addedSenderState.addPartitionMapping(emptyPartitionMap)
    context.become(active(partitionMappingState))

    partitionManagerMappings.foreach{ case (partitionId, partitionData, tableManager) =>
        tableManager ! AddPartition(partitionId, partitionData, partitionSize)
    }
  }

  private def handleTableAdded(state: CreateTableOperatorState, tableRef: ActorRef): Unit = {
    log.debug("handle table added")
    val newState = state.updateResult(tableRef)
    context.become(active(newState))

    val allTableManagers: Seq[ActorRef] = getAllTableManagers
    allTableManagers.foreach(tableManager => tableManager ! AddRemoteTable(tableName, tableRef))
  }

  private def handlePartitionCreated(state: CreateTableOperatorState, partitionId: Int, partition: ActorRef): Unit = {
    log.debug("handle partition created")
    val newState = state.addPartition(partitionId, partition)
    context.become(active(newState))

    if (newState.receivedAllPartitions()) {
      createTable(newState)
    }
  }

  private def createTable(state: CreateTableOperatorState): Unit = {
    log.debug("create table")
    // Select some TableManager randomly
    val allTableManagers: Seq[ActorRef] = getAllTableManagers
    val random = new Random()
    val chosenTableManager = allTableManagers(random.nextInt(allTableManagers.length))
    val columnNames = data.headOption.flatMap{ case (_, partitionData) => Some(partitionData.keys.toSeq)}.getOrElse(Seq.empty)

    chosenTableManager ! AddTable(tableName, columnNames, state.partitionMapping)
  }

  private def handleRemoteTableAdded(state: CreateTableOperatorState): Unit = {
    log.debug("handle remote table added")
    val newState = state.addRemoteTableAcknowledgement(sender())
    context.become(active(newState))

    if (newState.receivedAllRemoteAcknowledgements()) {
      state.originalSender ! QueryResult(newState.result)
    }
  }

  private def active(state: CreateTableOperatorState): Receive = {
    case Execute() => execute(state)
    case PartitionCreated(partitionId, partition) => handlePartitionCreated(state, partitionId, partition)
    case TableAdded(tableRef) => handleTableAdded(state, tableRef)
    case RemoteTableAdded() => handleRemoteTableAdded(state)
    case m => throw new Exception(s"Message not understood: $m")
  }
}