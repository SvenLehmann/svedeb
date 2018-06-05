package de.hpi.svedeb.operators

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class NestedLoopJoinOperatorTest extends AbstractActorTest("NestedLoopJoinOperator") {
  "A nested loop join operator" should "join two tables" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType("a", "b"))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType("b", "c"))))

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Map(0 -> Map("a" -> ColumnType("b"), "b" -> ColumnType("b"))))
  }

  it should "handle multiple columns" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType("a", "b"), "a2" -> ColumnType("x", "y"))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType("b", "c"), "b2" -> ColumnType("u", "v"))))

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    val expectedResult = Map(0 -> Map(
      "a" -> ColumnType("b"), "a2" -> ColumnType("y"),
      "b" -> ColumnType("b"), "b2" -> ColumnType("u")))
    checkTable(result.resultTable, expectedResult)
  }

  it should "handle multiple partitions" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType("a", "b")), Map("a" -> ColumnType("c", "d"))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType("b", "c")), Map("b" -> ColumnType("d", "e"))))

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Map(
      0 -> Map("a" -> ColumnType("b"), "b" -> ColumnType("b")),
      2 -> Map("a" -> ColumnType("c"), "b" -> ColumnType("c")),
      3 -> Map("a" -> ColumnType("d"), "b" -> ColumnType("d"))))
  }

  it should "handle multiple partitions (part2)" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType("a", "b", "c")), Map("a" -> ColumnType("d"))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType("b", "c")), Map("b" -> ColumnType("d", "e"))))

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Map(
      0 -> Map("a" -> ColumnType("b", "c"), "b" -> ColumnType("b", "c")),
      3 -> Map("a" -> ColumnType("d"), "b" -> ColumnType("d"))))
  }

  it should "handle inequality joins" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType("a", "b", "c"))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType("b", "c"))))

    val operator = system.actorOf(NestedLoopJoinOperator.props(leftTable, rightTable, "a", "b", _ < _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Map(0 -> Map("a" -> ColumnType("a", "a", "b"), "b" -> ColumnType("b", "c", "c"))))
  }
}
