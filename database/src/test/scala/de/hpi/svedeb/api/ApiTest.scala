package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.API.{ListRemoteAPIs, RemoteAPIs}
import de.hpi.svedeb.api.MaterializationWorker.{MaterializeTable, MaterializedTable}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._


// TODO: Implement
class ApiTest extends AbstractActorTest("Api") {
//  "An API" should "materialize a table" in {
//    val materializationWorker = TestProbe()
//    val user = TestProbe()
//    val table = TestProbe()
//    val tableManager = TestProbe()
//    val columns = Map("columnA" -> ColumnType("a", "b", "c"), "columnB" -> ColumnType("c", "b", "a"))
//    materializationWorker.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
//      case MaterializeTable(_) => sender ! MaterializedTable(user.ref, columns); TestActor.KeepRunning
//    })
//    val api = system.actorOf(API.props(tableManager.ref))
//    api ! Materialize(table.ref)
//
//    val materializedTable = expectMsgType[MaterializedResult]
//    materializedTable.result shouldEqual columns
//  }
  "An API" should "store remote APIs" in {
    val tableManager = TestProbe()
    val remoteAPI = TestProbe()
    val api = system.actorOf(API.props(tableManager.ref, Seq(remoteAPI.ref)))
    api ! ListRemoteAPIs()
    val remoteAPIs = expectMsgType[RemoteAPIs]
    remoteAPIs.remoteAPIs shouldEqual Seq(remoteAPI.ref)
  }
}
