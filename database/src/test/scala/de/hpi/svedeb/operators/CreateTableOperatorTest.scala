package de.hpi.svedeb.operators

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager.{AddTable, TableAdded}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.ColumnType

class CreateTableOperatorTest extends AbstractActorTest("CreateTableOperator") {

  "A CreateTableOperator" should "invoke creating in TableManager" in {
    val tableManager = TestProbe("TableManager")
    tableManager.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case AddTable(_, _, _) =>
        sender ! TableAdded(ActorRef.noSender)
        TestActor.KeepRunning
    })

    val createTableOperator = system.actorOf(CreateTableOperator.props(
      tableManager.ref,
      "SomeTable",
      Map(0 -> Map("columnA" -> ColumnType(), "columnB" -> ColumnType())),
      partitionSize = 10
    ))
    createTableOperator ! Execute()

    expectMsgType[QueryResult]
  }
}
