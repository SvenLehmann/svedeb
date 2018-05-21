package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.Column.{FilteredRowIndices, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Partition.{ColumnNameList, ColumnsRetrieved, GetColumns, ListColumnNames}
import de.hpi.svedeb.table.Table._
import de.hpi.svedeb.table.{Column, ColumnType, Partition, Table}
import org.scalatest.Matchers._

// TODO: Consider splitting up this test into multiple smaller ones that do not have so many dependencies
class ScanOperatorTest extends AbstractActorTest("ScanOperator") {

  "A ScanOperator actor" should "scan whole table" in {
    val table = TestProbe("Table")

    val scanOperator = system.actorOf(ScanOperator.props(table.ref, "a", _ => true))

    val partition = TestProbe("Partition")
    val columnA = TestProbe("ColumnA")
    val columnB = TestProbe("ColumnB")

    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnNames() => sender ! ColumnNameList(Seq("a", "b")); TestActor.KeepRunning
      case GetColumns() => sender ! ColumnsRetrieved(Map("a" -> columnA.ref, "b" -> columnB.ref)); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnsInTable() ⇒ sender ! ColumnList(Seq("a", "b")); TestActor.KeepRunning
      case GetColumnFromTable("ColumnA") => sender ! ActorsForColumn("ColumnA", Seq(columnA.ref)); TestActor.KeepRunning
      case GetColumnFromTable("ColumnB") => sender ! ActorsForColumn("ColumnB", Seq(columnB.ref)); TestActor.KeepRunning
      case GetPartitions() => sender! PartitionsInTable(Seq(partition.ref)); TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.FilterColumn(predicate) => sender ! FilteredRowIndices(0, "a", Seq(0, 1, 2)); TestActor.KeepRunning
      case Column.ScanColumn(_) => sender ! ScannedValues(0, "a", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })
    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(_) => sender ! ScannedValues(0, "b", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })

    scanOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Seq(Map("a" -> ColumnType("1", "2", "3"), "b" -> ColumnType("1", "2", "3"))))
  }

  it should "filter values without test probes" in {
    val partitionSize = 2
    val partition1 = system.actorOf(Partition.props(0, Map("columnA" -> ColumnType("a1", "a2"), "columnB" -> ColumnType("b1", "b2")), partitionSize))
    val partition2 = system.actorOf(Partition.props(1, Map("columnA" -> ColumnType("a3", "a4"), "columnB" -> ColumnType("b3", "b4")), partitionSize))
    val table = system.actorOf(Table.props(Seq("columnA", "columnB"), partitionSize, Seq(partition1, partition2)))
    val operator = system.actorOf(ScanOperator.props(table, "columnA", x => x.contains("1")))

    operator ! Execute()
    val msg = expectMsgType[QueryResult]

    checkTable(msg.resultTable, Seq(
      Map("columnA" -> ColumnType("a1"), "columnB" -> ColumnType("b1")),
      Map("columnA" -> ColumnType(), "columnB" -> ColumnType())))
  }
}
