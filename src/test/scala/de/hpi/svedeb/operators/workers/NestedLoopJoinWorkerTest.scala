package de.hpi.svedeb.operators.workers

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker.{JoinJob, PartialResult}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class NestedLoopJoinWorkerTest extends AbstractActorTest("NestedLoopJoinWorker") {

  "A NestedLoopJoin" should "join two columns" in {
    val leftPartition = generatePartitionTestProbe(0, Map("a" -> ColumnType("1", "2"), "b" -> ColumnType("b1", "b2")))._1
    val rightPartition = generatePartitionTestProbe(0, Map("c" -> ColumnType("2", "3"), "d" -> ColumnType("d2", "d3")))._1

    val joinWorker = system.actorOf(NestedLoopJoinWorker.props(leftPartition, rightPartition, 0, "a", "c", _ == _))

    joinWorker ! JoinJob()
    val workerResult = expectMsgType[PartialResult]
    checkPartition(workerResult.partition.get, Map(
      "a" -> ColumnType("2"),
      "b" -> ColumnType("b2"),
      "c" -> ColumnType("2"),
      "d" -> ColumnType("d2")
    ))
  }

  it should "return None if no matches" in {

    val leftPartition = generatePartitionTestProbe(0, Map("a" -> ColumnType("a", "b")))._1
    val rightPartition = generatePartitionTestProbe(0, Map("b" -> ColumnType("c", "d")))._1

    val joinWorker = system.actorOf(NestedLoopJoinWorker.props(leftPartition, rightPartition, 0, "a", "b", _ == _))

    joinWorker ! JoinJob()
    val workerResult = expectMsgType[PartialResult]
    workerResult.partition shouldEqual None
  }
}
