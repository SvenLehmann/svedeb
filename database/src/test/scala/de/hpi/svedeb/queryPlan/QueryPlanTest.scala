package de.hpi.svedeb.queryPlan

import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import org.scalatest.Matchers._

class QueryPlanTest extends AbstractActorTest("QueryPlan") {
  "QueryPlan" should "detect whether node has already been executed" in {
    val firstNode = GetTable("s")
    assert(!firstNode.isExecuted)

    val secondNode = Scan(firstNode, "a", _ => true)
    assert(!secondNode.isExecuted)

    val queryPlan = QueryPlan(secondNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    queryPlan.updateWorker(firstNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    assert(firstNode.isExecuted)
    assert(!secondNode.isExecuted)

    val thirdNode = Scan(secondNode, "b", _ => true)
    assert(!thirdNode.isExecuted)

    queryPlan.updateWorker(secondNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    assert(firstNode.isExecuted)
    assert(secondNode.isExecuted)
    assert(!thirdNode.isExecuted)

    val fourthNode = Scan(Scan(GetTable("SomeTable"), "a", _ == 1), "b", _ == 2)
    assert(!fourthNode.isExecuted)
  }

  it should "find correct next stage" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)

    val queryPlan = QueryPlan(secondNode)
    queryPlan.findNextStage() shouldEqual Some(firstNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    queryPlan.updateWorker(firstNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    queryPlan.findNextStage() shouldEqual Some(secondNode)
  }

  it should "find next stage for join" in {
    val firstTableNode = GetTable("s")
    val secondTableNode = GetTable("t")
    val joinNode = NestedLoopJoin(firstTableNode, secondTableNode, "a", "b", _ == _)

    val queryPlan = QueryPlan(joinNode)
    queryPlan.findNextStage() shouldEqual Some(firstTableNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    queryPlan.updateWorker(firstTableNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    queryPlan.findNextStage() shouldEqual Some(secondTableNode)

    val worker2 = TestProbe("worker2")
    queryPlan.updateWorker(secondTableNode, worker2.ref)
    queryPlan.saveIntermediateResult(worker2.ref, table.ref)

    queryPlan.findNextStage() shouldEqual Some(joinNode)

    val worker3 = TestProbe("worker3")
    queryPlan.updateWorker(joinNode, worker3.ref)
    queryPlan.saveIntermediateResult(worker3.ref, table.ref)

    queryPlan.findNextStage() shouldEqual None
  }

  it should "update the assigned worker" in {
    val node = GetTable("S")
    val queryPlan = QueryPlan(node)
    val worker = TestProbe("createTableWorker")
    queryPlan.updateWorker(node, worker.ref)

    node.assignedWorker.get shouldEqual worker.ref
  }

  it should "save intermediate result" in {
    val node = GetTable("S")
    val queryPlan = QueryPlan(node)
    val table = TestProbe("S")
    val worker = TestProbe("createTableWorker")

    queryPlan.updateWorker(node, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    node.resultTable.get shouldEqual table.ref
  }

  it should "enter next stage" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)

    val queryPlan = QueryPlan(secondNode)
    val worker = TestProbe("worker")
    val table = TestProbe("table")

    queryPlan.advanceToNextStage(worker.ref, table.ref, worker.ref, firstNode)

    firstNode.assignedWorker.get shouldEqual worker.ref
    firstNode.resultTable.get shouldEqual table.ref

    queryPlan.advanceToNextStage(worker.ref, table.ref, worker.ref, firstNode)
    firstNode.resultTable.get shouldEqual table.ref
  }

  it should "find node and update its worker" in {
    val getTableNode = GetTable("SomeTable")
    val firstScanNode = Scan(getTableNode, "SomeColumn", _ => true)
    val secondScanNode = Scan(firstScanNode, "SomeColumn", _ => true)
    val thirdScanNode = Scan(secondScanNode, "SomeColumn", _ => true)
    val queryPlan = QueryPlan(thirdScanNode)
    val worker = TestProbe("worker")

    queryPlan.updateWorker(firstScanNode, worker.ref)
    firstScanNode.assignedWorker.get shouldEqual worker.ref
  }

  it should "find node with sender" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)
    val queryPlan = QueryPlan(secondNode)
    val worker = TestProbe("worker")

    firstNode.assignedWorker = Some(worker.ref)
    assert(queryPlan.findNodeWithWorker(worker.ref).isDefined)
    queryPlan.findNodeWithWorker(worker.ref).get shouldEqual firstNode
  }
}
