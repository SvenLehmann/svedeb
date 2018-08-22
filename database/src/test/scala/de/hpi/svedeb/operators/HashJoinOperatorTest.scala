package de.hpi.svedeb.operators

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

class HashJoinOperatorTest extends AbstractActorTest("HashJoinOperator") {
  "A hash join operator" should "join two tables" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 2))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType(2, 3))))

    val operator = system.actorOf(HashJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Map(2 -> Map("a" -> ColumnType(2), "b" -> ColumnType(2))))
  }

  it should "handle multiple columns" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 2), "a2" -> ColumnType(4, 5))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType(2, 3), "b2" -> ColumnType(5, 6))))

    val operator = system.actorOf(HashJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    val expectedResult = Map(2 -> Map(
      "a" -> ColumnType(2), "a2" -> ColumnType(5),
      "b" -> ColumnType(2), "b2" -> ColumnType(5)))
    checkTable(result.resultTable, expectedResult)
  }

  it should "handle multiple partitions" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 2)), Map("a" -> ColumnType(3, 4))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType(2, 3)), Map("b" -> ColumnType(4, 5))))

    val operator = system.actorOf(HashJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]
    checkTable(result.resultTable, Map(
      2 -> Map("a" -> ColumnType(2), "b" -> ColumnType(2)),
      3 -> Map("a" -> ColumnType(3), "b" -> ColumnType(3)),
      4 -> Map("a" -> ColumnType(4), "b" -> ColumnType(4))))
  }

  it should "handle multiple partitions (part2)" in {

    val leftTable = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 2, 3)), Map("a" -> ColumnType(4))))
    val rightTable = generateTableTestProbe(Seq(Map("b" -> ColumnType(2, 3)), Map("b" -> ColumnType(4, 5))))

    val operator = system.actorOf(HashJoinOperator.props(leftTable, rightTable, "a", "b", _ == _))
    operator ! Execute()

    val result = expectMsgType[QueryResult]

    checkTable(result.resultTable, Map(
        2 -> Map("a" -> ColumnType(2), "b" -> ColumnType(2)),
        3 -> Map("a" -> ColumnType(3), "b" -> ColumnType(3)),
        4 -> Map("a" -> ColumnType(4), "b" -> ColumnType(4))))
  }
}
