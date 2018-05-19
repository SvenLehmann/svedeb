package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

class ProjectionOperatorTest extends AbstractActorTest("ProjectionOperator") {
  "A ProjectionOperator projecting one column" should "return a result table" in {
    val table = TestProbe("Table")
    val columnA = TestProbe("a")

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Nil => TestActor.KeepRunning
      case GetColumnFromTable("a") => sender ! ActorsForColumn("a", Seq(columnA.ref)); TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(0, "a", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })

    val projectionOperator = system.actorOf(ProjectionOperator.props(table.ref, Seq("a")))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Seq(
      Map("a" -> ColumnType("1", "2", "3"))))
  }

  it should "handle multiple partitions" in {
    val table = TestProbe("Table")
    val columnA0 = TestProbe("a")
    val columnA1 = TestProbe("a")
    val columnA2 = TestProbe("a")

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Nil => TestActor.KeepRunning
      case GetColumnFromTable("a") => sender ! ActorsForColumn("a", Seq(columnA0.ref, columnA1.ref, columnA2.ref)); TestActor.KeepRunning
    })

    columnA0.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(0, "a", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })

    columnA1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(1, "a", ColumnType("4", "5", "6")); TestActor.KeepRunning
    })

    columnA2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(2, "a", ColumnType("7", "8", "9")); TestActor.KeepRunning
    })

    val projectionOperator = system.actorOf(ProjectionOperator.props(table.ref, Seq("a")))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Seq(
      Map("a" -> ColumnType("1", "2", "3")),
      Map("a" -> ColumnType("4", "5", "6")),
      Map("a" -> ColumnType("7", "8", "9"))))
  }

  "A ProjectionOperator projecting multiple column" should "handle multiple partitions" in {
    val table = TestProbe("Table")
    val columnA0 = TestProbe("a")
    val columnA1 = TestProbe("a")
    val columnA2 = TestProbe("a")
    val columnB0 = TestProbe("b")
    val columnB1 = TestProbe("b")
    val columnB2 = TestProbe("b")

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case Nil => TestActor.KeepRunning
      case GetColumnFromTable("a") => sender ! ActorsForColumn("a", Seq(columnA0.ref, columnA1.ref, columnA2.ref)); TestActor.KeepRunning
      case GetColumnFromTable("b") => sender ! ActorsForColumn("b", Seq(columnB0.ref, columnB1.ref, columnB2.ref)); TestActor.KeepRunning
    })

    columnA0.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(0, "a", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })

    columnA1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(1, "a", ColumnType("4", "5", "6")); TestActor.KeepRunning
    })

    columnA2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(2, "a", ColumnType("7", "8", "9")); TestActor.KeepRunning
    })

    columnB0.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(0, "b", ColumnType("1", "2", "3")); TestActor.KeepRunning
    })

    columnB1.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(1, "b", ColumnType("4", "5", "6")); TestActor.KeepRunning
    })

    columnB2.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(None) => sender ! ScannedValues(2, "b", ColumnType("7", "8", "9")); TestActor.KeepRunning
    })

    val projectionOperator = system.actorOf(ProjectionOperator.props(table.ref, Seq("a", "b")))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Seq(
      Map("a" -> ColumnType("1", "2", "3"), "b" -> ColumnType("1", "2", "3")),
      Map("a" -> ColumnType("4", "5", "6"), "b" -> ColumnType("4", "5", "6")),
      Map("a" -> ColumnType("7", "8", "9"), "b" -> ColumnType("7", "8", "9"))))
  }
}
