package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.table.Table.{ColumnList, GetPartitions, ListColumnsInTable, PartitionsInTable}

class NestedLoopJoinOperatorTest extends AbstractActorTest("NestedLoopJoinOperator") {
  "A nested loop join operator" should "join two tables" in {
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

    val leftTable = TestProbe("LeftTable")
    leftTable.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() ⇒ sender.tell(PartitionsInTable(Seq(leftPartition.ref)), leftTable.ref); TestActor.KeepRunning
      case ListColumnsInTable() => sender.tell(ColumnList(Seq("a")), leftTable.ref); TestActor.KeepRunning
    })

    val rightTable = TestProbe("LeftTable")
    rightTable.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() ⇒ sender.tell(PartitionsInTable(Seq(rightPartition.ref)), rightTable.ref); TestActor.KeepRunning
      case ListColumnsInTable() => sender.tell(ColumnList(Seq("b")), rightTable.ref); TestActor.KeepRunning
    })

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable.ref, rightTable.ref, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Seq(Map("a" -> ColumnType("b"), "b" -> ColumnType("b"))))
  }

  it should "handle multiple columns" in {
    val leftColumn1 = TestProbe("LeftColumn1")
    leftColumn1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "a", ColumnType("a", "b")), leftColumn1.ref); TestActor.KeepRunning
      case ScanColumn(Some(_)) => sender ! ScannedValues(0, "a", ColumnType("b")); TestActor.KeepRunning
    })

    val leftColumn2 = TestProbe("LeftColumn2")
    leftColumn2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "a2", ColumnType("x", "y")), leftColumn2.ref); TestActor.KeepRunning
      case ScanColumn(Some(_)) => sender ! ScannedValues(0, "a2", ColumnType("y")); TestActor.KeepRunning
    })

    val rightColumn1 = TestProbe("RightColumn1")
    rightColumn1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "b", ColumnType("b", "c")), rightColumn1.ref); TestActor.KeepRunning
      case ScanColumn(Some(_)) => sender ! ScannedValues(0, "b", ColumnType("b")); TestActor.KeepRunning
    })

    val rightColumn2 = TestProbe("RightColumn2")
    rightColumn2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender.tell(ScannedValues(0, "b2", ColumnType("u", "v")), rightColumn2.ref); TestActor.KeepRunning
      case ScanColumn(Some(_)) => sender ! ScannedValues(0, "b2", ColumnType("v")); TestActor.KeepRunning
    })

    val leftPartition = TestProbe("LeftPartition")
    leftPartition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender.tell(ColumnsRetrieved(Map("a" -> leftColumn1.ref, "a2" -> leftColumn2.ref)), leftPartition.ref); TestActor.KeepRunning
    })

    val rightPartition = TestProbe("RightPartition")
    rightPartition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() ⇒ sender.tell(ColumnsRetrieved(Map("b" -> rightColumn1.ref, "b2" -> rightColumn2.ref)), rightPartition.ref); TestActor.KeepRunning
    })

    val leftTable = TestProbe("LeftTable")
    leftTable.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() ⇒ sender.tell(PartitionsInTable(Seq(leftPartition.ref)), leftTable.ref); TestActor.KeepRunning
      case ListColumnsInTable() => sender.tell(ColumnList(Seq("a")), leftTable.ref); TestActor.KeepRunning
    })

    val rightTable = TestProbe("LeftTable")
    rightTable.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() ⇒ sender.tell(PartitionsInTable(Seq(rightPartition.ref)), rightTable.ref); TestActor.KeepRunning
      case ListColumnsInTable() => sender.tell(ColumnList(Seq("b")), rightTable.ref); TestActor.KeepRunning
    })

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable.ref, rightTable.ref, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Seq(Map("a" -> ColumnType("b"), "a2" -> ColumnType("y"), "b" -> ColumnType("b"), "b2" -> ColumnType("v"))))
  }
}
