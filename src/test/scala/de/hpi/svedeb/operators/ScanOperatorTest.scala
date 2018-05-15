package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.QueryPlanExecutor.QueryFinished
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.queryplan.QueryPlan.Scan
import de.hpi.svedeb.table.Column.{FilteredRowIndizes, ScanColumn, ScannedValues}
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
      case ListColumnsInTable() ⇒ println("Received ListColumnsInTable"); sender ! ColumnList(Seq("a", "b")); TestActor.KeepRunning
      case GetColumnFromTable(name) => sender ! ActorsForColumn(Seq(columnA.ref, columnB.ref)); TestActor.KeepRunning
      case GetPartitions() => sender! PartitionsInTable(Seq(partition.ref)); TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.FilterColumn(predicate) => sender ! FilteredRowIndizes(0, "a", Seq(0, 1, 2)); TestActor.KeepRunning
      case Column.ScanColumn(_) => sender ! ScannedValues(0, "a", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })
    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(_) => sender ! ScannedValues(0, "b", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })

    scanOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]
    operatorResult.resultTable ! ListColumnsInTable()
    expectMsg(ColumnList(Seq("a", "b")))

    operatorResult.resultTable ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 })

    operatorResult.resultTable ! GetColumnFromTable("a")
    val returnedColumnA = expectMsgType[ActorsForColumn]
    returnedColumnA.columnActors.size shouldEqual 1

    returnedColumnA.columnActors.head ! ScanColumn()
    val scannedValuesA = expectMsgType[ScannedValues]
    scannedValuesA.values.size() shouldEqual 3
    scannedValuesA.values shouldEqual ColumnType("1", "2", "3")

    operatorResult.resultTable ! GetColumnFromTable("b")
    val returnedColumnB = expectMsgType[ActorsForColumn]
    returnedColumnB.columnActors.size shouldEqual 1

    returnedColumnB.columnActors.head ! ScanColumn()
    val scannedValuesB = expectMsgType[ScannedValues]
    scannedValuesB.values.size() shouldEqual 3
    scannedValuesB.values shouldEqual ColumnType("1", "2", "3")
  }

  it should "filter values without test probes" in {
    val partition1 = system.actorOf(Partition.props(0, Map("columnA" -> ColumnType("a1", "a2"), "columnB" -> ColumnType("b1", "b2")), 2))
    val partition2 = system.actorOf(Partition.props(0, Map("columnA" -> ColumnType("a3", "a4"), "columnB" -> ColumnType("b3", "b4")), 2))
    val table = system.actorOf(Table.props(Seq("columnA", "columnB"), 5, Seq(partition1, partition2)))
    val operator = system.actorOf(ScanOperator.props(table, "columnA", x => x.contains("1")))

    operator ! Execute()
    val msg = expectMsgType[QueryResult]


    val chainedOperator = system.actorOf(ScanOperator.props(msg.resultTable, "columnB", x => x.contains("2")))
    chainedOperator ! Execute
    val chainedResult = expectMsgType[QueryResult]

    chainedResult.resultTable ! GetColumnFromTable("columnA")
    val columnActors = expectMsgType[ActorsForColumn]
    columnActors.columnActors.foreach(column => {
      column ! ScanColumn(None)
      val columnContent = expectMsgType[ScannedValues]
      assert(columnContent.values.values.isEmpty)
      columnContent.values.values.foreach(value => assert(value.contains("1")))
    })

    msg.resultTable ! GetColumnFromTable("columnB")
    val columnActorsB = expectMsgType[ActorsForColumn]
    columnActorsB.columnActors.foreach(column => {
      column ! ScanColumn(None)
      val columnContent = expectMsgType[ScannedValues]
      columnContent.values.values.foreach(value => assert(value.contains("1")))
    })
  }
}
