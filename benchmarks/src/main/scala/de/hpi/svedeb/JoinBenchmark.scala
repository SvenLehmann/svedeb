package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.RowType

import scala.concurrent.Await
import scala.concurrent.duration._

object JoinBenchmark extends App {

  // Join Benchmark
  val api = SvedeB.start()
  implicit val timeout: Timeout = Timeout(3000 seconds)


  def testMaterialize(table: ActorRef): MaterializedResult = {
    val future = api.ask(Materialize(table))
    Await.result(future, timeout.duration).asInstanceOf[MaterializedResult]
  }

  def insertData(tableName: String, data: Seq[RowType]): Unit = {
    def insertRow(row: RowType): Result = {
      val queryPlanNode = InsertRow(GetTable(tableName), row)
      val queryFuture = api.ask(Query(QueryPlan(queryPlanNode)))
      Await.result(queryFuture, timeout.duration).asInstanceOf[Result]
    }

    data.foldLeft(Result(ActorRef.noSender))((_, row) => insertRow(row))
  }

  def loadData(tableName: String, columns: Seq[String], rowCount: Int): Unit = {
    val future = api.ask(Query(QueryPlan(CreateTable(tableName, columns, 100))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
    insertData(tableName, (0 to rowCount).map(i => RowType(i, i)))
  }

  def runQuery(name: String, function: => Result): Unit = {
    val resultTable = Utils.time(name, function)
    val materializedResult = Utils.time(s"Materializing of $name", testMaterialize(resultTable.resultTable))

    val rowCount = materializedResult.result.head._2.size()
    println(s"Result Rowcount of $name: $rowCount")
    println(materializedResult.result)
  }

  // Load 2 tables
  loadData("table1", Seq("columnA1", "columnB1"), 2000)
  loadData("table2", Seq("columnA2", "columnB2"), 2000)

  println("Finished loading")
  Thread.sleep(10000)

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
