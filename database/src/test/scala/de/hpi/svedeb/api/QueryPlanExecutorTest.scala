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
    val table = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 2))))

    val tableManager = generateTableManagerTestProbe(Seq(table))

    val queryPlan = QueryPlan(Scan(GetTable("SomeTable"), "a", _ => true))
    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager))
    queryPlanExecutor ! Run(0, queryPlan)

    val query = expectMsgType[QueryFinished]
    checkTable(query.resultTable, Map(0 -> Map("a" -> ColumnType(1, 2))))
  }

  it should "create an empty table" in {
    val table = generateTableTestProbe(Seq.empty)

    val tableManager = generateTableManagerTestProbe(Seq(table))

    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager))
    queryPlanExecutor ! Run(0, QueryPlan(CreateTable("SomeTable", Seq("a", "b"), 10)))

    val query = expectMsgType[QueryFinished]
    checkTable(query.resultTable, Map.empty)
  }

  it should "drop a table" in {
    val tableManager = generateTableManagerTestProbe(Seq.empty)

    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager))
    queryPlanExecutor ! Run(0, QueryPlan(DropTable("SomeTable")))

    expectMsgType[QueryFinished]
  }

  it should "query a non-empty table" in {

    val partition = TestProbe("partition")

    val table = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 1), "b" -> ColumnType(2, 2))))
    val tableManager = generateTableManagerTestProbe(Seq(table))

    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager))
    queryPlanExecutor ! Run(0, QueryPlan(Scan(Scan(GetTable("SomeTable"), "a", _ == 1), "b", _ == 2)))

    val resultTable = expectMsgType[QueryFinished]
    checkTable(resultTable.resultTable, Map(0 -> Map("a" -> ColumnType(1, 1), "b" -> ColumnType(2, 2))))
  }

  it should "insert a row" in {
    val table = TestProbe("table")

    val tableManager = generateTableManagerTestProbe(Seq(table.ref))

    table.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case AddRowToTable(_) => sender ! RowAddedToTable(); TestActor.KeepRunning
        }
    })
//    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
//      case AddRowToTable(_) => sender ! RowAddedToTable(); TestActor.KeepRunning
//    })

    val queryPlan = QueryPlan(InsertRow(GetTable("SomeTable"), RowType(1, 2)))
    val queryPlanExecutor = system.actorOf(QueryPlanExecutor.props(tableManager))
    queryPlanExecutor ! Run(0, queryPlan)

    table.expectMsgType[AddRowToTable]
    expectMsgType[QueryFinished]
  }
}
