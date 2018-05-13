package de.hpi.svedeb.queryPlan

import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.queryplan.QueryPlan.{GetTable, Scan}

class QueryPlanTest extends AbstractActorTest("QueryPlan") {
  "QueryPlan" should "find correct nextStep" in {
    val firstNode = GetTable("s")
    assert(firstNode.findNextStep() == firstNode)

    val secondNode = Scan(firstNode, "a", _ => true)
    assert(secondNode.findNextStep() == firstNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    firstNode.updateAssignedWorker(worker.ref)
    firstNode.saveIntermediateResult(worker.ref, table.ref)

    assert(secondNode.findNextStep() == secondNode)

    val thirdNode = Scan(secondNode, "b", _ => true)
    assert(thirdNode.findNextStep() == secondNode)

    secondNode.updateAssignedWorker(worker.ref)
    secondNode.saveIntermediateResult(worker.ref, table.ref)

    assert(thirdNode.findNextStep() == thirdNode)
  }

  it should "update the assigned worker" in {
    val node = GetTable("S")
    val worker = TestProbe("createTableWorker")
    node.updateAssignedWorker(worker.ref)

    assert(node.assignedWorker == worker.ref)
  }

  it should "save intermediate result" in {
    val node = GetTable("S")
    val table = TestProbe("S")
    val worker = TestProbe("createTableWorker")
    node.updateAssignedWorker(worker.ref)
    node.saveIntermediateResult(worker.ref, table.ref)

    assert(node.resultTable == table.ref)
  }
}
