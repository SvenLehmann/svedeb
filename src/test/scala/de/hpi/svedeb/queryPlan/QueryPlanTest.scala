package de.hpi.svedeb.queryPlan

import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.queryplan.QueryPlan.{GetTable, Scan}
import org.scalatest.Matchers._

class QueryPlanTest extends AbstractActorTest("QueryPlan") {
  "QueryPlan" should "detect whether node has already been executed" in {
    val firstNode = GetTable("s")
    assert(!firstNode.isExecuted)

    val secondNode = Scan(firstNode, "a", _ => true)
    assert(!secondNode.isExecuted)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    firstNode.findNodeAndUpdateWorker(firstNode, worker.ref)
    firstNode.saveIntermediateResult(worker.ref, table.ref)

    assert(firstNode.isExecuted)
    assert(!secondNode.isExecuted)

    val thirdNode = Scan(secondNode, "b", _ => true)
    assert(!thirdNode.isExecuted)

    secondNode.findNodeAndUpdateWorker(secondNode, worker.ref)
    secondNode.saveIntermediateResult(worker.ref, table.ref)

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

    thirdScanNode.findNode(fourthNode) shouldEqual None

    thirdScanNode.findNode(thirdScanNode) shouldEqual Some(thirdScanNode)
    thirdScanNode.findNode(secondScanNode) shouldEqual Some(secondScanNode)
    thirdScanNode.findNode(firstScanNode) shouldEqual Some(firstScanNode)
    thirdScanNode.findNode(getTableNode) shouldEqual Some(getTableNode)
  }

  it should "find correct next stage" in {
    val firstNode = GetTable("s")
    firstNode.findNextStage() shouldEqual Some(firstNode)

    val secondNode = Scan(firstNode, "a", _ => true)
    secondNode.findNextStage() shouldEqual Some(firstNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    firstNode.findNodeAndUpdateWorker(firstNode, worker.ref)
    firstNode.saveIntermediateResult(worker.ref, table.ref)

    secondNode.findNextStage() shouldEqual Some(secondNode)

    val thirdNode = Scan(secondNode, "b", _ => true)
    thirdNode.findNextStage() shouldEqual Some(secondNode)

    secondNode.findNodeAndUpdateWorker(secondNode, worker.ref)
    secondNode.saveIntermediateResult(worker.ref, table.ref)

    thirdNode.findNextStage() shouldEqual Some(thirdNode)
  }

  it should "update the assigned worker" in {
    val node = GetTable("S")
    val worker = TestProbe("createTableWorker")
    node.findNodeAndUpdateWorker(node, worker.ref)

    assert(node.assignedWorker == worker.ref)
  }

  it should "save intermediate result" in {
    val node = GetTable("S")
    val table = TestProbe("S")
    val worker = TestProbe("createTableWorker")

    node.findNodeAndUpdateWorker(node, worker.ref)
    node.saveIntermediateResult(worker.ref, table.ref)

    assert(node.resultTable == table.ref)
  }

  it should "enter next stage" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)
    val worker = TestProbe("worker")
    val table = TestProbe("table")

    firstNode.advanceToNextStage(worker.ref, table.ref, worker.ref, firstNode)

    assert(firstNode.assignedWorker == worker.ref)
    assert(firstNode.resultTable == table.ref)

    secondNode.advanceToNextStage(worker.ref, table.ref, worker.ref, firstNode)
    assert(firstNode.resultTable == table.ref)
  }

  it should "find node and update its worker" in {
    val getTableNode = GetTable("SomeTable")
    val firstScanNode = Scan(getTableNode, "SomeColumn", _ => true)
    val secondScanNode = Scan(firstScanNode, "SomeColumn", _ => true)
    val thirdScanNode = Scan(secondScanNode, "SomeColumn", _ => true)
    val worker = TestProbe("worker")

    val result = thirdScanNode.findNodeAndUpdateWorker(firstScanNode, worker.ref)
    result shouldEqual thirdScanNode
    result.findNode(firstScanNode).get.assignedWorker shouldEqual worker.ref
  }

  it should "find node with sender" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)
    val worker = TestProbe("worker")

    firstNode.assignedWorker = worker.ref
    assert(firstNode.findNodeWithWorker(worker.ref).isDefined)
    assert(firstNode.findNodeWithWorker(worker.ref).get == firstNode)
    assert(secondNode.findNodeWithWorker(worker.ref)isDefined)
    assert(secondNode.findNodeWithWorker(worker.ref).get == firstNode)
  }
}
