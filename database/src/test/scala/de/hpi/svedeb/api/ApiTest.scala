package de.hpi.svedeb.api

import de.hpi.svedeb.AbstractActorTest


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
}
