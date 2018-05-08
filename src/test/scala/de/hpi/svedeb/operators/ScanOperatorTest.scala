package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperatorWorker.QueryResult
import de.hpi.svedeb.operators.ScanOperator.Scan
import de.hpi.svedeb.table.{Column, ColumnType}
import de.hpi.svedeb.table.Column.{FilteredRowIndizes, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Partition.{ColumnNameList, ColumnsRetrieved, GetColumns, ListColumnNames}
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

class ScanOperatorTest extends AbstractActorTest("ScanOperator") {

  "A ScanOperator actor" should "scan whole table" in {
    val table = TestProbe()

    val scanOperator = system.actorOf(ScanOperator.props(table.ref))

    val partition = TestProbe()
    val columnA = TestProbe()
    val columnB = TestProbe()

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
      case Column.FilterColumn(predicate) => sender ! FilteredRowIndizes(Seq(0, 1, 2)); TestActor.KeepRunning
      case Column.ScanColumn(_) => sender ! ScannedValues("a", ColumnType(IndexedSeq("1", "2", "3"))); TestActor.KeepRunning
    })
    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(_) => sender ! ScannedValues("b", ColumnType(IndexedSeq("1", "2", "3"))); TestActor.KeepRunning
    })

    scanOperator ! Scan("a", _ => true)
    val operatorResult = expectMsgType[QueryResult]
    operatorResult.resultTable ! ListColumnsInTable()
    expectMsg(ColumnList(Seq("a", "b")))

    operatorResult.resultTable ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 })

    operatorResult.resultTable ! GetColumnFromTable("a")
    val returnedColumnA = expectMsgType[ActorsForColumn]
    returnedColumnA.columnActors.size shouldEqual 1

    returnedColumnA.columnActors.head ! ScanColumn(None)
    val scannedValuesA = expectMsgType[ScannedValues]
    scannedValuesA.values.size() shouldEqual 3
    scannedValuesA.values shouldEqual ColumnType(IndexedSeq("1", "2", "3"))

    operatorResult.resultTable ! GetColumnFromTable("b")
    val returnedColumnB = expectMsgType[ActorsForColumn]
    returnedColumnB.columnActors.size shouldEqual 1

    returnedColumnB.columnActors.head ! ScanColumn(None)
    val scannedValuesB = expectMsgType[ScannedValues]
    scannedValuesB.values.size() shouldEqual 3
    scannedValuesB.values shouldEqual ColumnType(IndexedSeq("1", "2", "3"))
  }
}
