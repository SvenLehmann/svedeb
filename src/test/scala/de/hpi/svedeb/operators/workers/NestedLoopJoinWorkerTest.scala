package de.hpi.svedeb.operators.workers

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.workers.NestedLoopJoinWorker.{JoinJob, PartialResult}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import org.scalatest.Matchers._

class NestedLoopJoinWorkerTest extends AbstractActorTest("NestedLoopJoinWorker") {

  "A NestedLoopJoin" should "join two columns" in {
    val leftColumn = TestProbe("LeftColumn")
    leftColumn.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "a", ColumnType("1", "2")), leftColumn.ref); TestActor.KeepRunning
      case ScanColumn(Some(Seq(1))) => sender ! ScannedValues(0, "a", ColumnType("2")); TestActor.KeepRunning
    })

    val leftColumn2 = TestProbe("LeftColumn2")
    leftColumn2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "b", ColumnType("b1", "b2")), leftColumn2.ref); TestActor.KeepRunning
      case ScanColumn(Some(Seq(1))) => sender ! ScannedValues(0, "b", ColumnType("b2")); TestActor.KeepRunning
    })

    val rightColumn = TestProbe("RightColumn")
    rightColumn.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "c", ColumnType("2", "3")), rightColumn.ref); TestActor.KeepRunning
      case ScanColumn(Some(Seq(0))) => sender ! ScannedValues(0, "c", ColumnType("2")); TestActor.KeepRunning
    })

    val rightColumn2 = TestProbe("RightColumn2")
    rightColumn2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "d", ColumnType("d2", "d3")), rightColumn2.ref); TestActor.KeepRunning
      case ScanColumn(Some(Seq(0))) => sender ! ScannedValues(0, "d", ColumnType("d2")); TestActor.KeepRunning
    })

    val leftPartition = TestProbe("LeftPartition")
    leftPartition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender.tell(ColumnsRetrieved(Map("a" -> leftColumn.ref, "b" -> leftColumn2.ref)), leftPartition.ref); TestActor.KeepRunning
    })

    val rightPartition = TestProbe("RightPartition")
    rightPartition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender.tell(ColumnsRetrieved(Map("c" -> rightColumn.ref, "d" -> rightColumn2.ref)), rightPartition.ref); TestActor.KeepRunning
    })

    val joinWorker = system.actorOf(NestedLoopJoinWorker.props(leftPartition.ref, rightPartition.ref, 0, "a", "c", _ == _))

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
    val leftColumn = TestProbe("LeftColumn")
    leftColumn.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "a", ColumnType("a", "b")), leftColumn.ref); TestActor.KeepRunning
    })

    val rightColumn = TestProbe("RightColumn")
    rightColumn.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "b", ColumnType("c", "d")), rightColumn.ref); TestActor.KeepRunning
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
    workerResult.partition shouldEqual None
  }
}
