package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Deploy, Props}
import akka.remote.RemoteScope
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.HashJoinOperator.{HashJoinState, JoinSide, LeftJoinSide, RightJoinSide}
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{MaterializeJoinResult, MaterializedJoinResult}
import de.hpi.svedeb.operators.workers.HashWorker.{HashJob, HashedTable}
import de.hpi.svedeb.operators.workers.ProbeWorker.{ProbeJob, ProbeResult}
import de.hpi.svedeb.operators.workers.{HashJoinMaterializationWorker, HashWorker, ProbeWorker}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table.{ColumnList, ListColumnsInTable}
import de.hpi.svedeb.utils.Utils.ValueType

import scala.util.Random

object HashJoinOperator {

  sealed trait JoinSide
  case object LeftJoinSide extends JoinSide
  case object RightJoinSide extends JoinSide

  private case class HashJoinState(originalSender: ActorRef,
                                   leftColumnNames: Option[Seq[String]],
                                   rightColumnNames: Option[Seq[String]],
                                   leftHashTable: Option[Map[ValueType, ActorRef]],
                                   rightHashTable: Option[Map[ValueType, ActorRef]],
                                   resultPartitionMap: Map[ValueType, Option[ActorRef]],
                                   startTime: Long,
                                   timeInHash: Long,
                                   timeInProbe: Long,
                                   timeInMaterialize: Long
                                  ) {
    def storeColumnNames(joinSide: JoinSide, columnNames: Seq[String]): HashJoinState = {
      joinSide match {
        case LeftJoinSide => HashJoinState(originalSender, Some(columnNames), rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap, startTime, timeInHash, timeInProbe, timeInMaterialize)
        case RightJoinSide => HashJoinState(originalSender, leftColumnNames, Some(columnNames), leftHashTable, rightHashTable, resultPartitionMap, startTime, timeInHash, timeInProbe, timeInMaterialize)
      }
    }

    def initializeResultMap(keys: Set[ValueType]): HashJoinState = {
      HashJoinState(originalSender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, keys.map((_, None)).toMap, startTime, timeInHash, timeInProbe, timeInMaterialize)
    }

    def hasReceivedAllResults: Boolean = {
      resultPartitionMap.forall(_._2.isDefined) && leftColumnNames.isDefined && rightColumnNames.isDefined
    }

    def storeSender(sender: ActorRef): HashJoinState = {
      HashJoinState(sender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap, startTime, timeInHash, timeInProbe, timeInMaterialize)
    }

    def storeMaterializedResult(hashKey: ValueType, partition: ActorRef): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap + (hashKey -> Some(partition)),
        startTime, timeInHash, timeInProbe, timeInMaterialize)
    }

    def storeHashTable(joinSide: JoinSide, hashTable: Map[ValueType, ActorRef]): HashJoinState = {
      if (joinSide == LeftJoinSide) {
        HashJoinState(originalSender,
          leftColumnNames, rightColumnNames,
          Some(hashTable), rightHashTable,
          resultPartitionMap,
          startTime, timeInHash, timeInProbe, timeInMaterialize)
      } else {
        HashJoinState(originalSender,
          leftColumnNames, rightColumnNames,
          leftHashTable, Some(hashTable),
          resultPartitionMap,
          startTime, timeInHash, timeInProbe, timeInMaterialize)
      }
    }

    def setStartTime(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, System.nanoTime(), timeInHash, timeInProbe, timeInMaterialize)
    }

    def setTimeInHash(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, startTime, System.nanoTime() - startTime, timeInProbe, timeInMaterialize)
    }

    def setTimeInProbe(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, startTime, timeInHash, System.nanoTime() - (startTime + timeInHash), timeInMaterialize)
    }

    def setTimeInMaterialize(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, startTime, timeInHash, timeInProbe, System.nanoTime() - (startTime + timeInHash + timeInProbe))
    }

    def hasFinishedHashPhaseAndReceivedColumnNames: Boolean =
      leftHashTable.isDefined && rightHashTable.isDefined &&
        leftColumnNames.isDefined && rightColumnNames.isDefined
  }

  def props(leftTable: ActorRef, rightTable: ActorRef, leftJoinColumn: String,
            rightJoinColumn: String,
            predicate: (ValueType, ValueType) => Boolean): Props =
    Props(new HashJoinOperator(leftTable, rightTable, leftJoinColumn, rightJoinColumn, predicate))
}
class HashJoinOperator(leftTable: ActorRef,
                       rightTable: ActorRef,
                       leftJoinColumn: String,
                       rightJoinColumn: String,
                       predicate: (ValueType, ValueType) => Boolean) extends AbstractOperator {
  override def receive: Receive = active(HashJoinState(ActorRef.noSender, None, None, None, None, Map.empty, 0, 0, 0, 0))

  private def initializeJoin(state: HashJoinState): Unit = {
    val startTimeState = state.setStartTime()
    context.become(active(startTimeState))

    val newState = startTimeState.storeSender(sender())
    context.become(active(newState))

    val leftHashWorker = context.actorOf(HashWorker
      .props(leftTable, leftJoinColumn, LeftJoinSide)
      .withDeploy(new Deploy(RemoteScope(leftTable.path.address))), "LeftHashWorker")
    val rightHashWorker = context.actorOf(HashWorker
      .props(rightTable, rightJoinColumn, RightJoinSide)
      .withDeploy(new Deploy(RemoteScope(rightTable.path.address))), "RightHashWorker")

    leftHashWorker ! HashJob()
    rightHashWorker ! HashJob()

    leftTable ! ListColumnsInTable()
    rightTable ! ListColumnsInTable()
  }

  private def storeHashResult(state: HashJoinState, hashMap: Map[ValueType, ActorRef], joinSide: JoinSide): Unit = {
    log.debug("Initiate Probe Phase")

    val newState = state.storeHashTable(joinSide, hashMap)
    context.become(active(newState))

    if (newState.hasFinishedHashPhaseAndReceivedColumnNames) {
      initiateProbePhase(newState)
    }
  }

  private def initiateProbePhase(state: HashJoinState): Unit = {
    val leftHashTable = state.leftHashTable.get
    val rightHashTable = state.rightHashTable.get

    val newState = state.setTimeInHash()
    context.become(active(newState))

    val random = new Random()

    val keys = for (
      key <- leftHashTable.keySet ++ rightHashTable.keySet;
      leftHashMap <- leftHashTable.get(key);
      rightHashMap <- rightHashTable.get(key)
    ) yield {
      log.debug(s"Starting ProbeWorker for hash $key")
      // Decide on which node we are instantiating the Worker Actor. Choose randomly between left and right partition.
      val address = if (random.nextBoolean()) {
        leftHashMap.path.address
      } else {
        rightHashMap.path.address
      }
      val worker = context.actorOf(ProbeWorker
        .props(key, leftHashMap, rightHashMap, predicate)
        .withDeploy(new Deploy(RemoteScope(address))), s"ProbeWorker$key")
      worker ! ProbeJob()
      key
    }
    context.become(active(newState.initializeResultMap(keys)))
  }

  private def handleProbeResult(state: HashJoinOperator.HashJoinState,
                                hashKey: ValueType): Unit = {
    log.debug("Received Probe Result, starting materialization")

//     TODO: is not exact, as we've here just seen one result, but not all. This will show the time of the slowest probe worker
    val newerState = state.setTimeInProbe()
    context.become(active(newerState))

    val columnNames = (newerState.leftColumnNames.get, newerState.rightColumnNames.get)
    context.actorOf(HashJoinMaterializationWorker.props(leftTable, rightTable, columnNames, hashKey, sender())
      .withDeploy(new Deploy(RemoteScope(sender().path.address))),
      s"HashJoinMaterializationWorker$hashKey") ! MaterializeJoinResult()
  }

  private def createResultTable(state: HashJoinState): Unit = {
    log.debug("create result table")

    // Assign new partition ids
    val partitions = state.resultPartitionMap
      .mapValues(_.get) // we just checked that all Options are set
      .filter{ case (_, partition) => partition != ActorRef.noSender}
      .map(identity) // convert MapLike to serializable Map

    val table = context.actorOf(Table.propsWithPartitions(
      state.leftColumnNames.get ++ state.rightColumnNames.get,
      partitions
    ))

    state.originalSender ! QueryResult(table)
  }

  private def handleMaterializedJoinResult(state: HashJoinState, hashKey: ValueType, partition: ActorRef): Unit = {
    log.debug("handle materialized join result")
    val newState = state.storeMaterializedResult(hashKey, partition)
    context.become(active(newState))

    if (newState.hasReceivedAllResults) {
      val newerState = newState.setTimeInMaterialize()
      println(s"${newerState.timeInHash} \t ${newerState.timeInProbe} \t ${newerState.timeInMaterialize}")

      createResultTable(newerState)
    }
  }

  private def handleColumnNames(state: HashJoinState, columnNames: Seq[String]): Unit = {
    val newState = if (sender() == leftTable) {
      state.storeColumnNames(LeftJoinSide, columnNames)
    } else if (sender() == rightTable){
      state.storeColumnNames(RightJoinSide, columnNames)
    } else {
      throw new Exception("Received column names from neither Left nor Right")
    }
    context.become(active(newState))

    if (newState.hasFinishedHashPhaseAndReceivedColumnNames) {
      initiateProbePhase(newState)
    }
  }

  private def active(state: HashJoinState): Receive = {
    case Execute() => initializeJoin(state)
    case HashedTable(hashMap, joinSide) => storeHashResult(state, hashMap, joinSide)
    case ProbeResult(hashKey) => handleProbeResult(state, hashKey)
    case ColumnList(columnNames) => handleColumnNames(state, columnNames)
    case MaterializedJoinResult(hashKey, partition) => handleMaterializedJoinResult(state, hashKey, partition)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
