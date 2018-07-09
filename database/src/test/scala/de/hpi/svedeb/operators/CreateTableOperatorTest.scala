package de.hpi.svedeb.operators

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.ColumnType

class CreateTableOperatorTest extends AbstractActorTest("CreateTableOperator") {

  "A CreateTableOperator" should "invoke creating in TableManager" in {
    val tableManager = generateTableManagerTestProbe(Seq.empty)

    val createTableOperator = system.actorOf(CreateTableOperator.props(
      tableManager,
      "SomeTable",
      Map(0 -> Map("columnA" -> ColumnType(), "columnB" -> ColumnType())),
      partitionSize = 10
    ))
    createTableOperator ! Execute()

    expectMsgType[QueryResult]
  }
}
