package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, Run}
import de.hpi.svedeb.management.TableManager._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Table.{ActorsForColumn, GetColumnFromTable}
import de.hpi.svedeb.table.{ColumnType, Partition, RowType, Table}
import org.scalatest.Matchers._
import scala.concurrent.duration._

class QueryPlanExecutorTest extends AbstractActorTest("APIWorker") {

  // TODO: This test should not actually invoke the whole query execution
  "An APIWorker" should "query an empty table" in {
    val tableManager = TestProbe("TableManager")
    val table = system.actorOf(Table.props(Seq("a", "b"), 10), name = "table")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table); TestActor.KeepRunning
      case AddTable(_, _) => sender ! TableAdded(table); TestActor.KeepRunning
      case RemoveTable(_) => sender ! TableRemoved(); TestActor.KeepRunning
    })

    val queryPlan = Scan(GetTable("SomeTable"), "a", _ => true)
//    val queryPlan = GetTable("S")
//    val queryPlan = CreateTable("SomeOtherTable", Seq("x", "y"))
//    val queryPlan = DropTable("SomeTable")
//    val queryPlan = InsertRow(GetTable("SomeTable"), RowType("elementA", "elementB"))
//    val queryPlan = Scan(Scan(GetTable("SomeTable"), "a", x => x == "x"), "b", x => x == "y")
    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref), name = "queryPlanExecutor")
    apiWorker ! Run(0, queryPlan)

    val query = expectMsgType[QueryFinished]
  }

  it should "create an empty table" in {
    val tableManager = TestProbe("TableManager")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddTable(_, _) => sender ! TableAdded(system.actorOf(Table.props(Seq("a"), 10))); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(0, CreateTable("SomeTable", Seq("a", "b")))

    val query = expectMsgType[QueryFinished]
  }

  it should "query a non-empty table" in {
    val tableManager = TestProbe("TableManager")
    val partition = system.actorOf(Partition.props(0, Map(("a", ColumnType("x", "x", "a")), ("b", ColumnType("y", "t", "y"))), 10))
    val table = system.actorOf(Table.props(Seq("a", "b"), 10, Seq(partition)))

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(0, Scan(Scan(GetTable("SomeTable"), "a", x => x == "x"), "b", x => x == "y"))

    val resultTable = expectMsgType[QueryFinished]

    resultTable.resultTable ! GetColumnFromTable("b")

    val resultColumnB = expectMsgType[ActorsForColumn]
    resultColumnB.columnActors.head ! ScanColumn()

    val contentColumnB= expectMsgType[ScannedValues]
    contentColumnB.values.values.size shouldEqual 1
    contentColumnB.values.values.head shouldEqual "y"

    resultTable.resultTable ! GetColumnFromTable("a")

    val resultColumnA = expectMsgType[ActorsForColumn]
    resultColumnA.columnActors.head ! ScanColumn()

    val contentColumnA = expectMsgType[ScannedValues]
    contentColumnA.values.values.size shouldEqual 1
    contentColumnA.values.values.head shouldEqual "x"
  }
}
