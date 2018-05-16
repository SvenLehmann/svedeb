package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.MaterializationWorker.{MaterializeTable, MaterializedTable}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Table.{ActorsForColumn, ColumnList, GetColumnFromTable, ListColumnsInTable}
import org.scalatest.Matchers._

class MaterializationWorkerTest extends AbstractActorTest("MaterializationWorker") {

  "A MaterializationWorker" should "materialize a simple table" in {

    val table = TestProbe("table")
    val columnA = TestProbe("columnA")
    val columnB = TestProbe("columnB")

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnsInTable() => sender.tell(ColumnList(Seq("columnA", "columnB")), table.ref); TestActor.KeepRunning
      case GetColumnFromTable(columnName) => columnName match {
        case "columnA" => sender ! ActorsForColumn("columnA", Seq(columnA.ref))
        case "columnB" => sender ! ActorsForColumn("columnB", Seq(columnB.ref))
      }; TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "columnA", ColumnType("a", "b", "c")); TestActor.KeepRunning
    })

    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "columnB", ColumnType("c", "b", "a")); TestActor.KeepRunning
    })

    val worker = system.actorOf(MaterializationWorker.props(self, ActorRef.noSender), "MaterializationWorker")
    worker ! MaterializeTable(table.ref)

    val result = expectMsgType[MaterializedTable]
    result.user shouldEqual ActorRef.noSender
    result.columns shouldEqual Map("columnA" -> ColumnType("a", "b", "c"), "columnB" -> ColumnType("c", "b", "a"))
  }

  it should "materialize a table with multiple partitions" in {

    val table = TestProbe("table")
    val columnA0 = TestProbe("columnA0")
    val columnA1 = TestProbe("columnA1")
    val columnA2 = TestProbe("columnA2")
    val columnB0 = TestProbe("columnB0")
    val columnB1 = TestProbe("columnB1")
    val columnB2 = TestProbe("columnB2")

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListColumnsInTable() => sender.tell(ColumnList(Seq("columnA", "columnB")), table.ref); TestActor.KeepRunning
      case GetColumnFromTable(columnName) => columnName match {
        case "columnA" => sender ! ActorsForColumn("columnA", Seq(columnA0.ref, columnA1.ref, columnA2.ref))
        case "columnB" => sender ! ActorsForColumn("columnB", Seq(columnB0.ref, columnB1.ref, columnB2.ref))
      }; TestActor.KeepRunning
    })

    columnA0.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "columnA", ColumnType("a", "b")); TestActor.KeepRunning
    })

    columnA1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(1, "columnA", ColumnType("c", "d")); TestActor.KeepRunning
    })

    columnA2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(2, "columnA", ColumnType("e", "f", "g")); TestActor.KeepRunning
    })

    columnB0.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "columnB", ColumnType("b", "a")); TestActor.KeepRunning
    })

    columnB1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(1, "columnB", ColumnType("f", "g")); TestActor.KeepRunning
    })

    columnB2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(2, "columnB", ColumnType("e", "d", "c")); TestActor.KeepRunning
    })

    val worker = system.actorOf(MaterializationWorker.props(self, ActorRef.noSender))
    worker ! MaterializeTable(table.ref)

    val result = expectMsgType[MaterializedTable]
    result.user shouldEqual ActorRef.noSender
    val valuesA = ColumnType("a", "b", "c", "d", "e", "f", "g")
    val valuesB = ColumnType("b", "a", "f", "g", "e", "d", "c")
    result.columns shouldEqual Map("columnA" -> valuesA, "columnB" -> valuesB)
  }

}
