package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{FetchTable, TableFetched}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import org.scalatest.Matchers._


class GetTableOperatorTest extends AbstractActorTest("GetTableOperator") {

  "A GetTableOperator" should "retrieve a table" in {
    val table = TestProbe("Table")

    val tableManager = generateTableManagerTestProbe(Seq(table.ref))

    val getTableOperator = system.actorOf(GetTableOperator.props(tableManager, "SomeTable"))
    getTableOperator ! Execute()

    val resultTable = expectMsgType[QueryResult]
    resultTable.resultTable shouldEqual table.ref
  }

}
