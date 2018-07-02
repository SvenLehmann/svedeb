package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.management.TableManager.{ListTables, TableList}
import de.hpi.svedeb.queryPlan.{CreateTable, QueryPlan}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class RemotingTest extends AbstractActorTest("Remoting") {

  ignore should "exchange table information" in {
    val firstInstance = SvedeB.start()
    val secondInstance = SvedeB.start(Seq(firstInstance.api), Seq(firstInstance.tableManager))

    val firstAPI = firstInstance.api
    val secondAPI = secondInstance.api

    firstAPI ! Query(QueryPlan(CreateTable("SomeTable", Map(0 -> Map("column" -> ColumnType())), 0)))
    expectMsgType[Result]

    def checkTableManager(tableManager: ActorRef): Unit = {
      tableManager ! ListTables()
      val tableList = expectMsgType[TableList]
      tableList.tableNames shouldEqual Seq("SomeTable")
    }

    // To avoid race conditions.. hacky fix
    Thread.sleep(5000)
    checkTableManager(firstInstance.tableManager)
    checkTableManager(secondInstance.tableManager)
  }

}
