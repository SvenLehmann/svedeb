package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperatorWorker.QueryResult
import de.hpi.svedeb.operators.ScanOperator.Scan
import de.hpi.svedeb.table.{Column, ColumnType}
import de.hpi.svedeb.table.Column.ScannedValues
import de.hpi.svedeb.table.Table._

import org.scalatest.Matchers._

class ScanOperatorTest extends AbstractActorTest("ScanOperator") {

  val tableManager = TestProbe()

  "A ScanOperator actor" should "scan whole table" in {
    val scanOperator = system.actorOf(ScanOperator.props(tableManager.ref))

    val table = TestProbe()
    val columnA = TestProbe()
    val columnB = TestProbe()

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(name) ⇒ sender ! TableFetched(table.ref); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnsInTable() ⇒ sender ! ColumnList(Seq("a", "b")); TestActor.KeepRunning
      case GetColumnFromTable(name) => sender ! ActorsForColumn(Seq(columnA.ref, columnB.ref)); TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(None) => sender ! ScannedValues("a", ColumnType(IndexedSeq("1", "2", "3"))); TestActor.KeepRunning
    })
    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(None) => sender ! ScannedValues("b", ColumnType(IndexedSeq("1", "2", "3"))); TestActor.KeepRunning
    })

    scanOperator ! Scan("SomeTable", "a", _ => true)
    val operatorResult = expectMsgType[QueryResult]
    operatorResult.resultTable ! ListColumnsInTable()
    expectMsg(ColumnList(Seq("a", "b")))

    operatorResult.resultTable ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 })

    operatorResult.resultTable ! GetColumnFromTable("a")
    val returnedColumnA = expectMsgType[ActorsForColumn]
    returnedColumnA.columnActors.size shouldEqual 1


  }
}
