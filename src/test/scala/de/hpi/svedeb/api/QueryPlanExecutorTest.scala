package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.QueryPlanExecutor.{Run, QueryFinished}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.table.Table

class QueryPlanExecutorTest extends AbstractActorTest("APIWorker") {

  // TODO: This test should not actually invoke the whole query execution
  "An APIWorker" should "query an empty table" in {
    val tableManager = TestProbe()
    val table = system.actorOf(Table.props(Seq("a", "b"), 10))

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(QueryPlanExecutor.props(tableManager.ref))
    // TODO: add actual query plan
    apiWorker ! Run(null)

    val query = expectMsgType[QueryFinished]
  }
}
