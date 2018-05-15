package de.hpi.svedeb

import akka.actor.PoisonPill

import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.RowType
import org.scalatest.Matchers._

import scala.annotation.tailrec
import scala.concurrent.duration._

object ExternalUser extends App {
  val api = SvedeB.start()

  implicit val timeout: Timeout = Timeout(30 seconds)

  def createInsertQuery(tableName: String, depth: Int): QueryPlanNode = {
    @tailrec
    def iter(depth: Int, queryPlanNode: QueryPlanNode): QueryPlanNode = {
      depth match {
        case 0 => queryPlanNode
        case d => iter(d - 1, InsertRow(queryPlanNode, RowType("a" + depth, "b" + depth)))
      }
    }
    iter(depth, GetTable(tableName))
  }

  def testMaterialize(): Unit = {
    val future1 = api.ask(Query(CreateTable("Table1", Seq("column1", "column2"), 100)))
    val result1 = Await.result(future1, timeout.duration).asInstanceOf[Result]

    val queryPlanNode = createInsertQuery("Table1", 100)
    val future2 = api.ask(Query(queryPlanNode))
    val insertedTable = Await.result(future2, timeout.duration).asInstanceOf[Result]
    val future3 = api.ask(Materialize(insertedTable.resultTable))
    val result3 = Await.result(future3, timeout.duration).asInstanceOf[MaterializedResult]

    result3.result.size shouldEqual 2
    val indexA = result3.result("column1").values(0).slice(1, result3.result("column1").values(0).length)
    val indexB = result3.result("column2").values(0).slice(1, result3.result("column2").values(0).length)
    indexA shouldEqual indexB

    println("finished creating")
  }

  def testDoubleScan(): Unit = {
    // Scan table
    val future4 = api.ask(Query(Scan(Scan(GetTable("Table1"), "column1", x => x.contains("1")), "column2", x => x.contains("2"))))
    val result4 = Await.result(future4, timeout.duration).asInstanceOf[Result]

    val future5 = api.ask(Materialize(result4.resultTable))
    val result5 = Await.result(future5, timeout.duration).asInstanceOf[MaterializedResult]

    result5.result("column1").values.foreach(value => assert(value.contains("1") && value.contains("2")))
    result5.result("column2").values.foreach(value => assert(value.contains("1") && value.contains("2")))

    println(result5.result)

    println("finished")
  }

  def testScan(): Unit = {
    // Scan table
    val queryFuture = api.ask(Query(Scan(GetTable("Table1"), "column1", x => x.contains("10"))))
    val queryResult = Await.result(queryFuture, timeout.duration).asInstanceOf[Result]

    val materializeFuture = api.ask(Materialize(queryResult.resultTable))
    val materializedResult = Await.result(materializeFuture, timeout.duration).asInstanceOf[MaterializedResult]

    println(materializedResult.result)

    println("finished")
  }

  testMaterialize()
  testDoubleScan()

  val before = System.nanoTime()
  testScan()
  val after = System.nanoTime()

  println("Time for single scan: {}ms", (after - before)/1000000)

  api ! Shutdown()


}
