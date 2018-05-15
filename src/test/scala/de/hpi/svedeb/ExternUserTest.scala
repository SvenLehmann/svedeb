import akka.actor.ActorRef

import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.{AbstractActorTest, SvedeB}
import de.hpi.svedeb.api.API.{Materialize, MaterializedResult, Query, Result}
import de.hpi.svedeb.queryplan.QueryPlan.{CreateTable, GetTable, InsertRow}
import de.hpi.svedeb.table.RowType

import scala.concurrent.duration._

class ExternUserTest extends AbstractActorTest("user") {
  "A user" should "be able to ask a query" in {
    val api = SvedeB.start()

    implicit val timeout: Timeout = Timeout(5 seconds)
    val future1 = api.ask(Query(CreateTable("Table1",Seq("column1", "column2"), 5)))
    val result1 = Await.result(future1, timeout.duration).asInstanceOf[Result]

    var insertedTable: Result = null
    (1 to 20).foreach( index => {
      val future2 = api.ask(Query(InsertRow(GetTable("Table1"), RowType("a" + index, "b" + index))))
      insertedTable = Await.result(future2, timeout.duration).asInstanceOf[Result]
    })
    val future3 = api.ask(Materialize(insertedTable.resultTable))
    val result3 = Await.result(future3, timeout.duration).asInstanceOf[MaterializedResult]

    print(result3)



  }
}
