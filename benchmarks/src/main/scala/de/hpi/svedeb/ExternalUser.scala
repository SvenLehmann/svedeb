package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.RowType

import scala.concurrent.Await
import scala.concurrent.duration._

object ExternalUser extends App {
  val api = SvedeB.start()

  implicit val timeout: Timeout = Timeout(30 seconds)

  def insertData(tableName: String, count: Int): Result = {
    def insertRow(id: Int): Result = {
      val queryPlanNode = InsertRow(GetTable(tableName), RowType(id, id))
      val queryFuture = api.ask(Query(QueryPlan(queryPlanNode)))
      Await.result(queryFuture, timeout.duration).asInstanceOf[Result]
    }

    (0 until count).foldLeft(Result(ActorRef.noSender))((_, id) => insertRow(id))
  }

  def loadData(): Unit = {
    val future = api.ask(Query(QueryPlan(CreateTable("Table1", Seq("column1", "column2"), 1000))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
    insertData("Table1", 100000)
  }

  def testMaterialize(table: ActorRef): MaterializedResult = {
    val future = api.ask(Materialize(table))
    Await.result(future, timeout.duration).asInstanceOf[MaterializedResult]
  }

  def testDoubleScan(): Result = {
    // Scan table
    val future = api.ask(Query(QueryPlan(Scan(Scan(GetTable("Table1"), "column1", _ == 1), "column2", _ == 2))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }

  def testScan(): Result = {
    // Scan table
    val queryFuture = api.ask(Query(QueryPlan(Scan(GetTable("Table1"), "column1", _ == 10))))
    Await.result(queryFuture, timeout.duration).asInstanceOf[Result]
  }

  def testGetTable(): Result = {
    val future = api.ask(Query(QueryPlan(GetTable("Table1"))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }

  def runQuery(name: String, function: => Result): Unit = {
    val resultTable = Utils.time(name, function)
    val materializedResult = Utils.time(s"Materializing of $name", testMaterialize(resultTable.resultTable))

    val rowCount = materializedResult.result.head._2.size()
    println(s"Result Rowcount of $name: $rowCount")
    println(materializedResult.result)
  }

  try {
    Utils.time("Load", loadData())
    runQuery("GetTable", testGetTable())
    runQuery("DoubleScan", testDoubleScan())
    runQuery("Scan", testScan())
  } finally {
    api ! Shutdown()
  }


}