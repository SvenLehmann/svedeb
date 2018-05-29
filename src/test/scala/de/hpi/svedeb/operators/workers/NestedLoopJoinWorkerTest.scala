package de.hpi.svedeb.operators.workers

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker.{JoinJob, PartialResult}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}

class NestedLoopJoinWorkerTest extends AbstractActorTest("NestedLoopJoinWorker") {

  "A NestedLoopJoin" should "join two columns" in {
    val leftColumn = TestProbe("LeftColumn")
    leftColumn.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "a", ColumnType("a", "b")), leftColumn.ref); TestActor.KeepRunning
      case ScanColumn(Some(_)) => sender ! ScannedValues(0, "a", ColumnType("b")); TestActor.KeepRunning
    })

    val rightColumn = TestProbe("RightColumn")
    rightColumn.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "b", ColumnType("b", "c")), rightColumn.ref); TestActor.KeepRunning
      case ScanColumn(Some(_)) => sender ! ScannedValues(0, "b", ColumnType("b")); TestActor.KeepRunning
    })

    val leftPartition = TestProbe("LeftPartition")
    leftPartition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender.tell(ColumnsRetrieved(Map("a" -> leftColumn.ref)), leftPartition.ref); TestActor.KeepRunning
    })

    val rightPartition = TestProbe("RightPartition")
    rightPartition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender.tell(ColumnsRetrieved(Map("b" -> rightColumn.ref)), rightPartition.ref); TestActor.KeepRunning
    })

    val joinWorker = system.actorOf(NestedLoopJoinWorker.props(leftPartition.ref, rightPartition.ref, 0, "a", "b", _ == _))

    joinWorker ! JoinJob()
    val workerResult = expectMsgType[PartialResult]
    checkPartition(workerResult.partition.get, Map("a" -> ColumnType("b"), "b" -> ColumnType("b")))
  }

}
