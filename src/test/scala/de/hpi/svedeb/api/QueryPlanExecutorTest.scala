package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, Run}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.Column.{FilterColumn, FilteredRowIndices, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

class QueryPlanExecutorTest extends AbstractActorTest("APIWorker") {
  
  "An APIWorker" should "query an empty table" in {
    val tableManager = TestProbe("TableManager")
    val table = TestProbe("table")
    val partition = TestProbe("partition")
    val column = TestProbe("column")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table.ref); TestActor.KeepRunning
      case AddTable(_, _, _) => sender ! TableAdded(table.ref); TestActor.KeepRunning
      case RemoveTable(_) => sender ! TableRemoved(); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() => sender ! PartitionsInTable(Seq(partition.ref)); TestActor.KeepRunning
      case ListColumnsInTable() => sender ! ColumnList(Seq("a")); TestActor.KeepRunning
    })

    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() => sender ! ColumnsRetrieved(Map("a" -> column.ref)); TestActor.KeepRunning
    })

    column.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "a", ColumnType("a", "b")); TestActor.KeepRunning
      case FilterColumn(_) => sender ! FilteredRowIndices(0, "a", Seq(0, 1)); TestActor.KeepRunning
    })

    val queryPlan = Scan(GetTable("SomeTable"), "a", _ => true)
    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref), name = "queryPlanExecutor")
    apiWorker ! Run(0, queryPlan)

    val query = expectMsgType[QueryFinished]
    checkTable(query.resultTable, Seq(Map("a" -> ColumnType("a", "b"))))
  }

  it should "create an empty table" in {
    val tableManager = TestProbe("TableManager")
    val table = TestProbe("table")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddTable(_, _, _) => sender ! TableAdded(table.ref); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() => sender ! PartitionsInTable(Seq()); TestActor.KeepRunning
      case ListColumnsInTable() => sender ! ColumnList(Seq("a", "b")); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(0, CreateTable("SomeTable", Seq("a", "b"), 10))

    val query = expectMsgType[QueryFinished]
    checkTable(query.resultTable, Seq())
  }

  it should "query a non-empty table" in {
    val tableManager = TestProbe("TableManager")

    val partition = TestProbe("partition")
    val columnA = TestProbe("columnA")
    val columnB = TestProbe("columnB")
    val table = TestProbe("table")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table.ref); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() => sender ! PartitionsInTable(Seq(partition.ref)); TestActor.KeepRunning
      case ListColumnsInTable() => sender ! ColumnList(Seq("a")); TestActor.KeepRunning
      case GetColumnFromTable("a") => sender ! ActorsForColumn("a", Seq(columnA.ref)); TestActor.KeepRunning
      case GetColumnFromTable("b") => sender ! ActorsForColumn("b", Seq(columnB.ref)); TestActor.KeepRunning
    })

    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() => sender ! ColumnsRetrieved(Map("a" -> columnA.ref, "b" -> columnB.ref)); TestActor.KeepRunning
    })

    columnA.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "a", ColumnType("x", "x")); TestActor.KeepRunning
      case FilterColumn(_) => sender ! FilteredRowIndices(0, "a", Seq(0, 1)); TestActor.KeepRunning
    })

    columnB.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "b", ColumnType("y", "y")); TestActor.KeepRunning
      case FilterColumn(_) => sender ! FilteredRowIndices(0, "b", Seq(0, 2)); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(0, Scan(Scan(GetTable("SomeTable"), "a", x => x == "x"), "b", x => x == "y"))

    val resultTable = expectMsgType[QueryFinished]
    checkTable(resultTable.resultTable, Seq(Map("a" -> ColumnType("x", "x"), "b" -> ColumnType("y", "y"))))
  }
}
