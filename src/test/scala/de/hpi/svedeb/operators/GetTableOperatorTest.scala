package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperatorWorker.{Execute, QueryResult}
import org.scalatest.Matchers._


class GetTableOperatorTest extends AbstractActorTest("GetTableOperator") {

  "A GetTableOperator" should "retrieve a table" in {
    val tableManager = TestProbe("TableManager")
    val table = TestProbe("Table")

    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case FetchTable(_) => sender ! TableFetched(table.ref); TestActor.KeepRunning
    })

    val getTableOperator = system.actorOf(GetTableOperator.props(tableManager.ref, "SomeTable"))
    getTableOperator ! Execute()

    val resultTable = expectMsgType[QueryResult]
    resultTable.resultTable shouldEqual table.ref
  }

}
