package de.hpi.svedeb

import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API.{Materialize, MaterializedResult, Query, Result}
import de.hpi.svedeb.queryplan.QueryPlan._
import de.hpi.svedeb.table.RowType
import org.scalatest.Matchers._

import scala.concurrent.duration._

object ExternalUser extends App {
  val api = SvedeB.start()

  implicit val timeout: Timeout = Timeout(30 seconds)
  val future1 = api.ask(Query(CreateTable("Table1", Seq("column1", "column2"), 5)))
  val result1 = Await.result(future1, timeout.duration).asInstanceOf[Result]

  val future2 = api.ask(Query(createInsertQuery(10)))
  val insertedTable = Await.result(future2, timeout.duration).asInstanceOf[Result]
  val future3 = api.ask(Materialize(insertedTable.resultTable))
  val result3 = Await.result(future3, timeout.duration).asInstanceOf[MaterializedResult]

  result3.result.size shouldEqual 2
  val indexA = result3.result("column1").values(0).slice(1, result3.result("column1").values(0).length)
  val indexB = result3.result("column2").values(0).slice(1, result3.result("column2").values(0).length)
  indexA shouldEqual indexB

  print("finished creating")

  // Scan table
  val future4 = api.ask(Query(Scan(Scan(GetTable("Table1"), "column1", x => x.contains("1")), "column2", x => x.contains("2"))))
  val result4 = Await.result(future1, timeout.duration).asInstanceOf[Result]

  print("start materialze")

  val future5 = api.ask(Materialize(insertedTable.resultTable))
  val result5 = Await.result(future3, timeout.duration).asInstanceOf[MaterializedResult]

  print("asserts")
  result5.result("column1").values.foreach(value => assert(value.contains("1") && value.contains("2")))
  result5.result("column2").values.foreach(value => assert(value.contains("1") && value.contains("2")))

  print("finished")

  def createInsertQuery(depth: Int): QueryPlanNode = {
    if (depth == 0) return GetTable("Table1")
    InsertRow(createInsertQuery(depth - 1), RowType("a" + depth, "b" + depth))
  }
}
