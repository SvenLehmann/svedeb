package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.RowType
import de.hpi.svedeb.table.Table.{AddRowToTable, RowAddedToTable}

class InsertRowOperatorTest extends AbstractActorTest("InsertRowOperator") {

  "An InsertRowOperator" should "add a row" in {
    val table = TestProbe()
    table.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddRowToTable(_) => sender ! RowAddedToTable(); TestActor.KeepRunning
    })

    val insertOperator = system.actorOf(InsertRowOperator.props(table.ref, RowType(1, 2)))
    insertOperator ! Execute()

    expectMsgType[QueryResult]
  }
}
