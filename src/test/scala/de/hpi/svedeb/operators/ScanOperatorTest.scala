package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.ScanOperator.Scan
import de.hpi.svedeb.table.{Column, ColumnType}
import de.hpi.svedeb.table.Column.ScannedValues
import de.hpi.svedeb.table.Table.{ActorsForColumn, ColumnList, GetColumnFromTable, ListColumnsInTable}

class ScanOperatorTest extends AbstractActorTest("ScanOperator") {

  val tableManager = TestProbe()

  "A ScanOperator actor" should "scan whole table" in {
    val scanOperator = system.actorOf(ScanOperator.props(tableManager.ref))
    scanOperator ! Scan("SomeTable")

    val table = TestProbe()
    val columnA = TestProbe()
    val columnB = TestProbe()

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(name) ⇒ sender ! TableFetched(table.ref); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnsInTable() ⇒ sender ! ColumnList(List("a", "b")); TestActor.KeepRunning
      case GetColumnFromTable(name) => sender ! ActorsForColumn(List(columnA.ref, columnB.ref)); TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(None) => sender ! ScannedValues("a", ColumnType(IndexedSeq("1", "2", "3"))); TestActor.KeepRunning
    })
    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Column.ScanColumn(None) => sender ! ScannedValues("a", ColumnType(IndexedSeq("1", "2", "3"))); TestActor.KeepRunning
    })


  }
}
