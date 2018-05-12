package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, Run}
import de.hpi.svedeb.management.TableManager.{AddTable, FetchTable, TableAdded, TableFetched}
import de.hpi.svedeb.queryplan.QueryPlan.{CreateTable, GetTable, Scan}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Table.{ActorsForColumn, GetColumnFromTable}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
import org.scalatest.Matchers._

class QueryPlanExecutorTest extends AbstractActorTest("APIWorker") {

  // TODO: This test should not actually invoke the whole query execution
  "An APIWorker" should "query an empty table" in {
    val tableManager = TestProbe("TableManager")
    val table = system.actorOf(Table.props(Seq("a", "b"), 10))

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(Vector(GetTable("SomeTable"), Scan("a", _ => true)))

    val query = expectMsgType[QueryFinished]
  }

  it should "create an empty table" in {
    val tableManager = TestProbe("TableManager")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddTable(_, _) => sender ! TableAdded(ActorRef.noSender); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(Vector(CreateTable("SomeTable", Seq("a", "b"))))

    val query = expectMsgType[QueryFinished]
  }

  it should "query a non-empty table" in {
    val tableManager = TestProbe("TableManager")
    val partition = system.actorOf(Partition.props(Map(("a", ColumnType("x", "x", "a")), ("b", ColumnType("y", "t", "y"))), 10))
    val table = system.actorOf(Table.props(Seq("a", "b"), 10, Seq(partition)))

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    apiWorker ! Run(Vector(GetTable("SomeTable"), Scan("a", x => x == "x"), Scan("b", x => x == "y")))

    val resultTable = expectMsgType[QueryFinished]
    resultTable.resultTable ! GetColumnFromTable("a")

    val resultColumnA = expectMsgType[ActorsForColumn]
    resultColumnA.columnActors.head ! ScanColumn(None)

    val contentColumnA = expectMsgType[ScannedValues]
    contentColumnA.values.values.size shouldEqual 1
    contentColumnA.values.values.head shouldEqual "x"

    resultTable.resultTable ! GetColumnFromTable("b")

    val resultColumnB = expectMsgType[ActorsForColumn]
    resultColumnB.columnActors.head ! ScanColumn(None)

    val contentColumnB= expectMsgType[ScannedValues]
    contentColumnB.values.values.size shouldEqual 1
    contentColumnB.values.values.head shouldEqual "y"

  }
}
