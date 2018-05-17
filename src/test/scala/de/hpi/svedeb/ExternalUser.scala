package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.RowType

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._

object ExternalUser extends App {
  val api = SvedeB.start()

  implicit val timeout: Timeout = Timeout(30 seconds)

  def insertData(tableName: String, count: Int): Result = {
    def insertRow(id: Int): Result = {
      val queryPlanNode = InsertRow(GetTable(tableName), RowType("a" + id, "b" + id))
      val queryFuture = api.ask(Query(queryPlanNode))
      Await.result(queryFuture, timeout.duration).asInstanceOf[Result]
    }

    (0 until count).foldLeft(Result(ActorRef.noSender))((_, id) => insertRow(id))
  }

  def loadData(): Unit = {
    val future1 = api.ask(Query(CreateTable("Table1", Seq("column1", "column2"), 1000)))
    Await.result(future1, timeout.duration).asInstanceOf[Result]
    insertData("Table1", 100000)
  }

  def testMaterialize(table: ActorRef): MaterializedResult = {
    val future3 = api.ask(Materialize(table))
    Await.result(future3, timeout.duration).asInstanceOf[MaterializedResult]
  }

  def testDoubleScan(): Result = {
    // Scan table
    val future4 = api.ask(Query(Scan(Scan(GetTable("Table1"), "column1", x => x.contains("1")), "column2", x => x.contains("2"))))
    Await.result(future4, timeout.duration).asInstanceOf[Result]
  }

  def testScan(): Result = {
    // Scan table
    val queryFuture = api.ask(Query(Scan(GetTable("Table1"), "column1", x => x.contains("10"))))
    Await.result(queryFuture, timeout.duration).asInstanceOf[Result]
  }

  def getTable(): Result = {
    val getTableFuture = api.ask(Query(GetTable("Table1")))
    Await.result(getTableFuture, timeout.duration).asInstanceOf[Result]
  }

  def time[R](description: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time in $description: ${(t1 - t0)/1000000.0}ms")
    result
  }

  def runQuery(name: String, function: => Result): Unit = {
    val resultTable = time(name, function)
    val materializedResult = time(s"Materializing of $name", testMaterialize(resultTable.resultTable))

    val rowCount = materializedResult.result.head._2.size()
    println(s"Result Rowcount of $name: $rowCount")
    println(materializedResult.result)
  }

  try {
    time("Load", loadData())
    runQuery("GetTable", getTable())
    runQuery("DoubleScan", testDoubleScan())
    runQuery("Scan", testScan())
  } finally {
    api ! Shutdown()
  }


}
