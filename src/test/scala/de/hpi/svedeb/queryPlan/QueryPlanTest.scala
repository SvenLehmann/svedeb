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
    queryPlan.findNodeAndUpdateWorker(firstNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    assert(firstNode.isExecuted)
    assert(!secondNode.isExecuted)

    val thirdNode = Scan(secondNode, "b", _ => true)
    assert(!thirdNode.isExecuted)

    queryPlan.findNodeAndUpdateWorker(secondNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    assert(firstNode.isExecuted)
    assert(secondNode.isExecuted)
    assert(!thirdNode.isExecuted)

    val fourthNode = Scan(Scan(GetTable("SomeTable"), "a", x => x == "x"), "b", x => x == "y")
    assert(!fourthNode.isExecuted)
  }

  it should "find a node" in {
    val getTableNode = GetTable("SomeTable")
    val firstScanNode = Scan(getTableNode, "SomeColumn", _ => true)
    val secondScanNode = Scan(firstScanNode, "SomeColumn", _ => true)
    val thirdScanNode = Scan(secondScanNode, "SomeColumn", _ => true)
    val fourthNode = GetTable("SomeOtherTable")

    val queryPlan = QueryPlan(thirdScanNode)

    queryPlan.findNode(fourthNode) shouldEqual None

    queryPlan.findNode(thirdScanNode) shouldEqual Some(thirdScanNode)
    queryPlan.findNode(secondScanNode) shouldEqual Some(secondScanNode)
    queryPlan.findNode(firstScanNode) shouldEqual Some(firstScanNode)
    queryPlan.findNode(getTableNode) shouldEqual Some(getTableNode)
  }

  it should "find correct next stage" in {
    val firstNode = GetTable("s")

    val secondNode = Scan(firstNode, "a", _ => true)

    val queryPlan = QueryPlan(secondNode)
    queryPlan.findNextStage() shouldEqual Some(firstNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    queryPlan.findNodeAndUpdateWorker(firstNode, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    queryPlan.findNextStage() shouldEqual Some(secondNode)
  }

  it should "update the assigned worker" in {
    val node = GetTable("S")
    val queryPlan = QueryPlan(node)
    val worker = TestProbe("createTableWorker")
    queryPlan.findNodeAndUpdateWorker(node, worker.ref)

    assert(node.assignedWorker == worker.ref)
  }

  it should "save intermediate result" in {
    val node = GetTable("S")
    val queryPlan = QueryPlan(node)
    val table = TestProbe("S")
    val worker = TestProbe("createTableWorker")

    queryPlan.findNodeAndUpdateWorker(node, worker.ref)
    queryPlan.saveIntermediateResult(worker.ref, table.ref)

    assert(node.resultTable == table.ref)
  }

  it should "enter next stage" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)

    val queryPlan = QueryPlan(secondNode)
    val worker = TestProbe("worker")
    val table = TestProbe("table")

    queryPlan.advanceToNextStage(worker.ref, table.ref, worker.ref, firstNode)

    assert(firstNode.assignedWorker == worker.ref)
    assert(firstNode.resultTable == table.ref)

    queryPlan.advanceToNextStage(worker.ref, table.ref, worker.ref, firstNode)
    assert(firstNode.resultTable == table.ref)
  }

  it should "find node and update its worker" in {
    val getTableNode = GetTable("SomeTable")
    val firstScanNode = Scan(getTableNode, "SomeColumn", _ => true)
    val secondScanNode = Scan(firstScanNode, "SomeColumn", _ => true)
    val thirdScanNode = Scan(secondScanNode, "SomeColumn", _ => true)
    val queryPlan = QueryPlan(thirdScanNode)
    val worker = TestProbe("worker")

    queryPlan.findNodeAndUpdateWorker(firstScanNode, worker.ref)
    queryPlan.findNode(firstScanNode).get.assignedWorker shouldEqual worker.ref
  }

  it should "find node with sender" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)
    val queryPlan = QueryPlan(secondNode)
    val worker = TestProbe("worker")

    firstNode.assignedWorker = worker.ref
    assert(queryPlan.findNodeWithWorker(worker.ref).isDefined)
    assert(queryPlan.findNodeWithWorker(worker.ref).get == firstNode)
  }
}
