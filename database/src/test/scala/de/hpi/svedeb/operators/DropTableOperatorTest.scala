package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{AddTable, RemoveTable, TableAdded, TableRemoved}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import org.scalatest.Matchers._

class DropTableOperatorTest extends AbstractActorTest("DropTableOperator") {

  "A DropTableOperator" should "invoke dropping in TableManager" in {
    val tableManager = generateTableManagerTestProbe(Seq.empty)

    val dropTableOperator = system.actorOf(DropTableOperator.props(tableManager, "SomeTable"))
    dropTableOperator ! Execute()

    val result = expectMsgType[QueryResult]
    result.resultTable shouldEqual ActorRef.noSender
  }
}
