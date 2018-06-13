package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.{ColumnType, RowType}
import de.hpi.svedeb.table.Table.{AddRowToTable, RowAddedToTable}

class InsertRowOperatorTest extends AbstractActorTest("InsertRowOperator") {

  "An InsertRowOperator" should "add a row" in {
    val table = generateTableTestProbe(Seq(Map("a" -> ColumnType(), "b" -> ColumnType())))

    val insertOperator = system.actorOf(InsertRowOperator.props(table, RowType(1, 2)))
    insertOperator ! Execute()

    expectMsgType[QueryResult]
  }
}
