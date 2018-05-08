package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{RemoveTable, TableRemoved}
import de.hpi.svedeb.operators.AbstractOperatorWorker.{Execute, QueryResult}

class DropTableOperatorTest extends AbstractActorTest("DropTableOperator") {

  "A DropTableOperator" should "invoke dropping in TableManager" in {
    val tableManager = TestProbe("TableManager")
    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case RemoveTable(_) => sender ! TableRemoved(); TestActor.KeepRunning
    })

    val dropTableOperator = system.actorOf(DropTableOperator.props(tableManager.ref, "SomeTable"))
    dropTableOperator ! Execute()

    expectMsgType[QueryResult]
  }
}
