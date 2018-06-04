package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, Run}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.Column.{FilterColumn, FilteredRowIndices, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.{ColumnType, RowType}
import de.hpi.svedeb.table.Partition.{ColumnsRetrieved, GetColumns}
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

class QueryPlanExecutorTest extends AbstractActorTest("APIWorker") {
  
  "An QueryPlanExecutor" should "query an empty table" in {
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
      case GetPartitions() => sender ! PartitionsInTable(Map(0 -> partition.ref)); TestActor.KeepRunning
      case ListColumnsInTable() => sender ! ColumnList(Seq("a")); TestActor.KeepRunning
    })

    partition.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetColumns() => sender ! ColumnsRetrieved(Map("a" -> column.ref)); TestActor.KeepRunning
    })

    column.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ScanColumn(_) => sender ! ScannedValues(0, "a", ColumnType("a", "b")); TestActor.KeepRunning
      case FilterColumn(_) => sender ! FilteredRowIndices(0, "a", Seq(0, 1)); TestActor.KeepRunning
    })

    val queryPlan = QueryPlan(Scan(GetTable("SomeTable"), "a", _ => true))
    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    queryPlanExecutor ! Run(0, queryPlan)

    val query = expectMsgType[QueryFinished]
    checkTable(query.resultTable, Map(0 -> Map("a" -> ColumnType("a", "b"))))
  }

  it should "create an empty table" in {
    val tableManager = TestProbe("TableManager")
    val table = TestProbe("table")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddTable(_, _, _) => sender ! TableAdded(table.ref); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case GetPartitions() => sender ! PartitionsInTable(Map.empty); TestActor.KeepRunning
      case ListColumnsInTable() => sender ! ColumnList(Seq("a", "b")); TestActor.KeepRunning
    })

    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    queryPlanExecutor ! Run(0, QueryPlan(CreateTable("SomeTable", Seq("a", "b"), 10)))

    val query = expectMsgType[QueryFinished]
    checkTable(query.resultTable, Map.empty)
  }

  it should "drop a table" in {
    val tableManager = TestProbe("TableManager")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case RemoveTable("SomeTable") => sender ! TableRemoved(); TestActor.KeepRunning
    })

    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    queryPlanExecutor ! Run(0, QueryPlan(DropTable("SomeTable")))

    expectMsgType[QueryFinished]
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
      case GetPartitions() => sender ! PartitionsInTable(Map(0 -> partition.ref)); TestActor.KeepRunning
      case ListColumnsInTable() => sender ! ColumnList(Seq("a")); TestActor.KeepRunning
      case GetColumnFromTable("a") => sender ! ActorsForColumn("a", Map(0 -> columnA.ref)); TestActor.KeepRunning
      case GetColumnFromTable("b") => sender ! ActorsForColumn("b", Map(0 -> columnB.ref)); TestActor.KeepRunning
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

    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    queryPlanExecutor ! Run(0, QueryPlan(Scan(Scan(GetTable("SomeTable"), "a", _ == "x"), "b", _ == "y")))

    val resultTable = expectMsgType[QueryFinished]
    checkTable(resultTable.resultTable, Map(0 -> Map("a" -> ColumnType("x", "x"), "b" -> ColumnType("y", "y"))))
  }

  it should "insert a row" in {
    val tableManager = TestProbe("TableManager")
    val table = TestProbe("table")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table.ref); TestActor.KeepRunning
    })

    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddRowToTable(_) => sender ! RowAddedToTable(); TestActor.KeepRunning
    })

    val queryPlan = QueryPlan(InsertRow(GetTable("SomeTable"), RowType("newA", "newB")))
    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    queryPlanExecutor ! Run(0, queryPlan)

    table.expectMsgType[AddRowToTable]
    expectMsgType[QueryFinished]
  }
}
