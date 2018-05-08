package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{AddTable, TableAdded}
import de.hpi.svedeb.operators.AbstractOperatorWorker.{Execute, QueryResult}

class CreateTableOperatorTest extends AbstractActorTest("CreateTableOperator") {

  "A CreateTableOperator" should "invoke creating in TableManager" in {
    val tableManager = TestProbe("TableManager")
    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddTable(_, _) => sender ! TableAdded(ActorRef.noSender); TestActor.KeepRunning
    })

    val createTableOperator = system.actorOf(CreateTableOperator.props(tableManager.ref, "SomeTable", Seq("columnA", "columnB")))
    createTableOperator ! Execute()

    expectMsgType[QueryResult]
  }
}
