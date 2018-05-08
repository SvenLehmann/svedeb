package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.APIWorker.{Execute, QueryFinished}
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.table.Table

class APIWorkerTest extends AbstractActorTest("APIWorker") {

  "An APIWorker" should "query an empty table" in {
    val tableManager = TestProbe()
    val table = system.actorOf(Table.props(Seq("a", "b"), 10))

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table); TestActor.KeepRunning
    })

    val apiWorker = system.actorOf(APIWorker.props(tableManager.ref))
    apiWorker ! Execute()

    val query = expectMsgType[QueryFinished]
  }
}