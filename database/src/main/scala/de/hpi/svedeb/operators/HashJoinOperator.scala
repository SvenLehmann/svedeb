package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Deploy, Props}
import akka.remote.RemoteScope
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.HashJoinOperator.{HashJoinState, JoinSide, LeftJoinSide, RightJoinSide}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{MaterializeJoinResult, MaterializedJoinResult}
import de.hpi.svedeb.operators.workers.{HashJoinMaterializationWorker, HashWorker, ProbeWorker}
import de.hpi.svedeb.operators.workers.HashWorker.{HashJob, HashedTable}
import de.hpi.svedeb.operators.workers.ProbeWorker.{ProbeJob, ProbeResult}
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
                                   timeBeforeHash: Long,
                                   timeBeforeProbe: Long,
                                   timeBeforeMaterialize: Long
                                  ) {
    def storeColumnNames(joinSide: JoinSide, columnNames: Seq[String]): HashJoinState = {
      joinSide match {
        case LeftJoinSide => HashJoinState(originalSender, Some(columnNames), rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap, timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
        case RightJoinSide => HashJoinState(originalSender, leftColumnNames, Some(columnNames), leftHashTable, rightHashTable, resultPartitionMap, timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
      }
    }

    def initializeResultMap(keys: Set[ValueType]): HashJoinState = {
      HashJoinState(originalSender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, keys.map((_, None)).toMap, timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
    }

    def hasReceivedAllResults: Boolean = {
      resultPartitionMap.forall(_._2.isDefined) && leftColumnNames.isDefined && rightColumnNames.isDefined
    }

    def storeSender(sender: ActorRef): HashJoinState = {
      HashJoinState(sender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap, timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
    }

    def storeMaterializedResult(hashKey: ValueType, partition: ActorRef): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap + (hashKey -> Some(partition)),
        timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
    }

    def storeHashTable(joinSide: JoinSide, hashTable: Map[ValueType, ActorRef]): HashJoinState = {
      if (joinSide == LeftJoinSide) {
        HashJoinState(originalSender,
          leftColumnNames, rightColumnNames,
          Some(hashTable), rightHashTable,
          resultPartitionMap,
          timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
      } else {
        HashJoinState(originalSender,
          leftColumnNames, rightColumnNames,
          leftHashTable, Some(hashTable),
          resultPartitionMap,
          timeBeforeHash, timeBeforeProbe, timeBeforeMaterialize)
      }
    }

    def setTimeBeforeHash(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, System.nanoTime(), timeBeforeProbe, timeBeforeMaterialize)
    }

    def setTimeBeforeProbe(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, timeBeforeHash, System.nanoTime(), timeBeforeMaterialize)
    }

    def setTimeBeforeMaterialize(): HashJoinState = {
      HashJoinState(originalSender,
        leftColumnNames, rightColumnNames,
        leftHashTable, rightHashTable,
        resultPartitionMap, timeBeforeHash, timeBeforeProbe, System.nanoTime())
    }

    def hasFinishedHashAndReceivedColumnNamesPhase: Boolean =
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
  override def receive: Receive = active(HashJoinState(ActorRef.noSender, None, None, None, None, Map.empty, 0, 0, 0))

  private def initializeJoin(state: HashJoinState): Unit = {
    val newState = state.storeSender(sender())
    context.become(active(newState))

    val leftHashWorker = context.actorOf(HashWorker
      .props(leftTable, leftJoinColumn, LeftJoinSide)
      .withDeploy(new Deploy(RemoteScope(leftTable.path.address))), "LeftHashWorker")
    val rightHashWorker = context.actorOf(HashWorker
      .props(rightTable, rightJoinColumn, RightJoinSide)
      .withDeploy(new Deploy(RemoteScope(rightTable.path.address))), "RightHashWorker")

    val newerState = newState.setTimeBeforeHash()
    context.become(active(newerState))

    leftHashWorker ! HashJob()
    rightHashWorker ! HashJob()

    leftTable ! ListColumnsInTable()
    rightTable ! ListColumnsInTable()
  }

  private def storeHashResult(state: HashJoinState, hashMap: Map[ValueType, ActorRef], joinSide: JoinSide): Unit = {
    log.debug("Initiate Probe Phase")

    val newState = state.storeHashTable(joinSide, hashMap)
    context.become(active(newState))

    if (newState.hasFinishedHashAndReceivedColumnNamesPhase) {
      initiateProbePhase(newState)
    }
  }

  private def initiateProbePhase(state: HashJoinState): Unit = {
    val left = state.leftHashTable.get
    val right = state.rightHashTable.get

    val newState = state.setTimeBeforeProbe()
    context.become(active(newState))

    val keys = for (
      key <- left.keySet ++ right.keySet;
      leftHashMap <- left.get(key);
      rightHashMap <- right.get(key)
    ) yield {
      log.debug(s"Starting ProbeWorker for hashkey $key")
      // Decide on which node we are instantiating the Worker Actor. Choose randomly between left and right partition.
      val random = new Random()
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
                                hashKey: ValueType,
                                indices: Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)]): Unit = {
    log.debug("Received Probe Result, starting materialization")

    val newerState = state.setTimeBeforeMaterialize()
    context.become(active(newerState))

    val columnNames = (newerState.leftColumnNames.get, newerState.rightColumnNames.get)
    context.actorOf(HashJoinMaterializationWorker.props(leftTable, rightTable, columnNames, hashKey, indices)
      .withDeploy(new Deploy(RemoteScope(sender().path.address))),
      s"HashJoinMaterializationWorker$hashKey") ! MaterializeJoinResult()
  }

  def createResultTable(state: HashJoinState): Unit = {
    log.debug("create result table")

    // Assign new partition ids
    val partitions = state.resultPartitionMap
      .mapValues(_.get).filter{ case (_, partition) => partition != ActorRef.noSender}.map(identity) // we just checked that all Options are set

    val table = context.actorOf(Table.propsWithPartitions(
      state.leftColumnNames.get ++ state.rightColumnNames.get,
      partitions
    ))

    state.originalSender ! QueryResult(table)
  }

  def handleMaterializedJoinResult(state: HashJoinState, hashKey: ValueType, partition: ActorRef): Unit = {
    log.debug("handle materialized join result")
    val newState = state.storeMaterializedResult(hashKey, partition)
    context.become(active(newState))

    if (newState.hasReceivedAllResults) {
      val timeNow = System.nanoTime()
//      println(s"Total time from before hash: ${(timeNow - newState.timeBeforeHash)/1000000.0}ms")
//      println(s"Total time from before probe: ${(timeNow - newState.timeBeforeProbe)/1000000.0}ms")
//      println(s"Total time from before materialize: ${(timeNow - newState.timeBeforeMaterialize)/1000000.0}ms")

      createResultTable(newState)
    }
  }

  def handleColumnNames(state: HashJoinState, columnNames: Seq[String]): Unit = {
    val newState = if (sender() == leftTable) {
      state.storeColumnNames(LeftJoinSide, columnNames)
    } else if (sender() == rightTable){
      state.storeColumnNames(RightJoinSide, columnNames)
    } else {
      state
    }

    context.become(active(newState))

    if (newState.hasFinishedHashAndReceivedColumnNamesPhase) {
      initiateProbePhase(newState)
    }
  }

  private def active(state: HashJoinState): Receive = {
    case Execute() => initializeJoin(state)
    case HashedTable(hashMap, joinSide) => storeHashResult(state, hashMap, joinSide)
    case ProbeResult(hashKey, indices) => handleProbeResult(state, hashKey, indices)
    case ColumnList(columnNames) => handleColumnNames(state, columnNames)
    case MaterializedJoinResult(hashKey, partition) => handleMaterializedJoinResult(state, hashKey, partition)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
