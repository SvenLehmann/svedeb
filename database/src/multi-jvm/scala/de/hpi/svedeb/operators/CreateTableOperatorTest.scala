package de.hpi.svedeb.operators

import akka.actor.{ActorIdentity, Identify}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import de.hpi.svedeb.STMultiNodeSpec
import de.hpi.svedeb.management.TableManager
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
    import CreateTableOperatorTestConfig._

    runOn(node1) {
      enterBarrier("deployed")
      val tableManagerSelection = system.actorSelection(node(node2) / "user" / "TableManager")

      tableManagerSelection ! Identify()
      val tableManagerIdentity = expectMsgType[ActorIdentity]
      val tableManager = tableManagerIdentity.ref.get

      val createTableOperator = system.actorOf(CreateTableOperator.props(
        tableManager,
        "SomeTable",
        Map(0 -> Map("columnA" -> ColumnType(), "columnB" -> ColumnType())),
        10
      ))

      createTableOperator ! Execute()
      expectMsgType[QueryResult]
    }

    runOn(node2) {
      system.actorOf(TableManager.props(), "TableManager")
      enterBarrier("deployed")
    }
    enterBarrier("finished")
  }
}
