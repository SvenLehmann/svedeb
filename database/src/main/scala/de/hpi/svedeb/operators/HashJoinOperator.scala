package de.hpi.svedeb.operators

import akka.actor.{ActorRef, Props}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators.HashJoinOperator.{HashJoinState, JoinSide, LeftJoinSide, RightJoinSide}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.HashJoinMaterializationWorker.{MaterializeJoinResult, MaterializedJoinResult}
import de.hpi.svedeb.operators.workers.{HashJoinMaterializationWorker, HashWorker, ProbeWorker}
import de.hpi.svedeb.operators.workers.HashWorker.{HashJob, HashedTable}
import de.hpi.svedeb.operators.workers.ProbeWorker.{ProbeJob, ProbeResult}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table.ColumnList
import de.hpi.svedeb.utils.Utils.ValueType

object HashJoinOperator {

  sealed trait JoinSide
  case object LeftJoinSide extends JoinSide
  case object RightJoinSide extends JoinSide

  private case class HashJoinState(originalSender: ActorRef,
                                   leftColumnNames: Option[Seq[String]],
                                   rightColumnNames: Option[Seq[String]],
                                   leftHashTable: Option[Map[ValueType, ActorRef]],
                                   rightHashTable: Option[Map[ValueType, ActorRef]],
                                   resultPartitionMap: Map[ValueType, Option[ActorRef]]
                                  ) {
    def storeColumnNames(joinSide: JoinSide, columnNames: Seq[String]): HashJoinState = {
      joinSide match {
        case LeftJoinSide => HashJoinState(originalSender, Some(columnNames), rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap)
        case RightJoinSide => HashJoinState(originalSender, leftColumnNames, Some(columnNames), leftHashTable, rightHashTable, resultPartitionMap)
      }
    }

    def initializeResultMap(keys: Set[ValueType]): HashJoinState = {
      HashJoinState(originalSender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, keys.map((_, None)).toMap)
    }

    def hasReceivedAllResults: Boolean = {
      resultPartitionMap.forall(_._2.isDefined)
    }

    def storeSender(sender: ActorRef): HashJoinState = {
      HashJoinState(sender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap)
    }

    def storeMaterializedResult(hashKey: ValueType, partition: ActorRef): HashJoinState = {
      if (partition == ActorRef.noSender) {
        HashJoinState(originalSender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap + (hashKey -> None))
      } else {
        HashJoinState(originalSender, leftColumnNames, rightColumnNames, leftHashTable, rightHashTable, resultPartitionMap + (hashKey -> Some(partition)))
      }
    }

    def storeHashTable(joinSide: JoinSide, hashTable: Map[ValueType, ActorRef]): HashJoinState = {
      if (joinSide == LeftJoinSide) {
        HashJoinState(originalSender, leftColumnNames, rightColumnNames, Some(hashTable), rightHashTable, resultPartitionMap)
      } else {
        HashJoinState(originalSender, leftColumnNames, rightColumnNames, leftHashTable, Some(hashTable), resultPartitionMap)
      }
    }

    def hasFinishedHashPhase: Boolean = leftHashTable.isDefined && rightHashTable.isDefined
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
  override def receive: Receive = active(HashJoinState(ActorRef.noSender, None, None, None, None, Map.empty))

  private def initializeJoin(state: HashJoinState): Unit = {
    val leftHashWorker = context.actorOf(HashWorker.props(leftTable, leftJoinColumn, LeftJoinSide))
    val rightHashWorker = context.actorOf(HashWorker.props(rightTable, rightJoinColumn, RightJoinSide))
    leftHashWorker ! HashJob()
    rightHashWorker ! HashJob()
  }

  private def storeHashResult(state: HashJoinState, hashMap: Map[ValueType, ActorRef], joinSide: JoinSide): Unit = {
    log.info("Initiate Probe Phase")

    val newState = state.storeHashTable(joinSide, hashMap)
    context.become(active(newState))

    if (newState.hasFinishedHashPhase) {
      val left = newState.leftHashTable.get
      val right = newState.rightHashTable.get

      for (
        key <- left.keySet ++ right.keySet;
        leftHashMap <- left.get(key);
        rightHashMap <- right.get(key)
      ) yield {
        val worker = context.actorOf(ProbeWorker.props(key, leftHashMap, rightHashMap, predicate))
        worker ! ProbeJob()
      }
      val keys = left.keySet ++ right.keySet
      context.become(active(newState.initializeResultMap(keys)))
    }
  }

  private def handleProbeResult(state: HashJoinOperator.HashJoinState,
                                hashKey: ValueType,
                                indices: Seq[(PartitionedHashTableEntry, PartitionedHashTableEntry)]): Unit = {
    context.actorOf(HashJoinMaterializationWorker.props(leftTable, rightTable, hashKey, indices)) ! MaterializeJoinResult()
  }

  def handleMaterializedJoinResult(state: HashJoinState, hashKey: ValueType, partition: ActorRef): Unit = {
    val newState = state.storeMaterializedResult(hashKey, partition)
    context.become(active(newState))

    if (newState.hasReceivedAllResults) {

      // Assign new partition ids
      val partitions = newState.resultPartitionMap.values
        .filter(_.isDefined)
        .map(_.get)
        .zipWithIndex
        .map{ case (p, id) => id -> p }
        .toMap

      val table = context.actorOf(Table.propsWithPartitions(
        newState.leftColumnNames.get ++ newState.rightColumnNames.get,
        partitions
      ))

      newState.originalSender ! QueryResult(table)
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
