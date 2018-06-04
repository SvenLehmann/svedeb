package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.RowType

import scala.concurrent.Await
import scala.concurrent.duration._

object Benchmarks extends App {

  // Join Benchmark
  val api = SvedeB.start()
  implicit val timeout: Timeout = Timeout(30 seconds)


  def time[R](description: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time in $description: ${(t1 - t0)/1000000.0}ms")
    result
  }

  def testMaterialize(table: ActorRef): MaterializedResult = {
    val future = api.ask(Materialize(table))
    Await.result(future, timeout.duration).asInstanceOf[MaterializedResult]
  }

  def insertData(tableName: String, count: Int): Result = {
    def insertRow(id: Int): Result = {
      val queryPlanNode = InsertRow(GetTable(tableName), RowType(s"a$id", s"b$id"))
      val queryFuture = api.ask(Query(QueryPlan(queryPlanNode)))
      Await.result(queryFuture, timeout.duration).asInstanceOf[Result]
    }

    (0 until count).foldLeft(Result(ActorRef.noSender))((_, id) => insertRow(id))
  }

  def loadData(tableName: String, columns: Seq[String], rowCount: Int): Unit = {
    val future = api.ask(Query(QueryPlan(CreateTable(tableName, columns, 1000))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
    insertData(tableName, rowCount)
  }

  def runQuery(name: String, function: => Result): Unit = {
    val resultTable = time(name, function)
    val materializedResult = time(s"Materializing of $name", testMaterialize(resultTable.resultTable))

    val rowCount = materializedResult.result.head._2.size()
    println(s"Result Rowcount of $name: $rowCount")
    println(materializedResult.result)
  }

  // Load 2 tables
  loadData("table1", Seq("columnA1", "columnB1"), 2000)
  loadData("table2", Seq("columnA2", "columnB2"), 2000)

  println("Finished loading")

  def testJoin(): Result = {
    // Perform Join
    val future = api.ask(Query(
      QueryPlan(
        NestedLoopJoin(
          GetTable("table1"),
          GetTable("table2"),
          "columnA1",
          "columnA2",
          _ == _)
      )
    ))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }



  try {
    runQuery("Join", testJoin())
  } catch {
    case e: Exception => println(e)
  } finally {
    api ! Shutdown()
  }

}
