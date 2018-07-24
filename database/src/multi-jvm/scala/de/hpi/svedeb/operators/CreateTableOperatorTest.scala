package de.hpi.svedeb.operators

import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.ColumnType

object CreateTableOperatorTestConfig extends MultiNodeConfig {
  val node1: RoleName = role("node1")
  val node2: RoleName = role("node2")
}

class CreateTableOperatorTestMultiJvmNode1 extends CreateTableOperatorTest
class CreateTableOperatorTestMultiJvmNode2 extends CreateTableOperatorTest

class CreateTableOperatorTest extends MultiNodeSpec(CreateTableOperatorTestConfig)
  with STMultiNodeSpec with ImplicitSender {

  def initialParticipants: Int = roles.size

  //TODO: Test uses Akka Cluster, must be multi-node test
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
