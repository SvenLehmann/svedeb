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
    firstNode.updateAssignedWorker(firstNode, worker.ref)
    firstNode.saveIntermediateResult(worker.ref, table.ref)

    assert(firstNode.isExecuted)
    assert(!secondNode.isExecuted)

    val thirdNode = Scan(secondNode, "b", _ => true)
    assert(!thirdNode.isExecuted)

    secondNode.updateAssignedWorker(secondNode, worker.ref)
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

  it should "find correct next step" in {
    val firstNode = GetTable("s")
    firstNode.findNextStep() shouldEqual Some(firstNode)

    val secondNode = Scan(firstNode, "a", _ => true)
    secondNode.findNextStep() shouldEqual Some(firstNode)

    val table = TestProbe("S")
    val worker = TestProbe("worker")
    firstNode.updateAssignedWorker(firstNode, worker.ref)
    firstNode.saveIntermediateResult(worker.ref, table.ref)

    secondNode.findNextStep() shouldEqual Some(secondNode)

    val thirdNode = Scan(secondNode, "b", _ => true)
    thirdNode.findNextStep() shouldEqual Some(secondNode)

    secondNode.updateAssignedWorker(secondNode, worker.ref)
    secondNode.saveIntermediateResult(worker.ref, table.ref)

    thirdNode.findNextStep() shouldEqual Some(thirdNode)
  }

  it should "update the assigned worker" in {
    val node = GetTable("S")
    val worker = TestProbe("createTableWorker")
    node.updateAssignedWorker(node, worker.ref)

    assert(node.assignedWorker == worker.ref)
  }

  it should "find node with worker" in {

  }

  it should "save intermediate result" in {
    val node = GetTable("S")
    val table = TestProbe("S")
    val worker = TestProbe("createTableWorker")

    node.updateAssignedWorker(node, worker.ref)
    node.saveIntermediateResult(worker.ref, table.ref)

    assert(node.resultTable == table.ref)
  }

  it should "enter next stage" in {
    val firstNode = GetTable("s")
    val secondNode = Scan(firstNode, "a", _ => true)
    val worker = TestProbe("worker")
    val table = TestProbe("table")

    firstNode.advanceToNextStep(worker.ref, table.ref, worker.ref, firstNode)

    assert(firstNode.assignedWorker == worker.ref)
    assert(firstNode.resultTable == table.ref)

    secondNode.advanceToNextStep(worker.ref, table.ref, worker.ref, firstNode)
    assert(firstNode.resultTable == table.ref)
  }

  it should "find node and update its worker" in {
    val getTableNode = GetTable("SomeTable")
    val firstScanNode = Scan(getTableNode, "SomeColumn", _ => true)
    val secondScanNode = Scan(firstScanNode, "SomeColumn", _ => true)
    val thirdScanNode = Scan(secondScanNode, "SomeColumn", _ => true)
    val worker = TestProbe("worker")

    val result = thirdScanNode.findNodeAndUpdateWorker(worker.ref, firstScanNode)
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
