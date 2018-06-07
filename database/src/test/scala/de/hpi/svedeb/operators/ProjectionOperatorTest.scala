package de.hpi.svedeb.operators

import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
import org.scalatest.Matchers._

class ProjectionOperatorTest extends AbstractActorTest("ProjectionOperator") {

  "A ProjectionOperator projection zero columns" should "return an empty result table" in {
    val table = TestProbe("Table")
    val projectionOperator = system.actorOf(ProjectionOperator.props(table.ref, Seq()))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Map.empty)
  }

  "A ProjectionOperator projecting one column" should "return a result table" in {
    val table = generateTableTestProbe(Seq(Map("a" -> ColumnType(1, 2, 3))))
    val projectionOperator = system.actorOf(ProjectionOperator.props(table, Seq("a")))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Map(
      0 -> Map("a" -> ColumnType(1, 2, 3))))
  }

  it should "handle multiple partitions" in {
    val table = generateTableTestProbe(Seq(
      Map("a" -> ColumnType(1, 2, 3)),
      Map("a" -> ColumnType(4, 5, 6)),
      Map("a" -> ColumnType(7, 8, 9))))

    val projectionOperator = system.actorOf(ProjectionOperator.props(table, Seq("a")))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Map(
      0 -> Map("a" -> ColumnType(1, 2, 3)),
      1 -> Map("a" -> ColumnType(4, 5, 6)),
      2 -> Map("a" -> ColumnType(7, 8, 9))))
  }

  it should "work without test probes" in {
    val partitionSize = 2
    val partition1 = system.actorOf(Partition.props(0, Map("columnA" -> ColumnType(1, 2), "columnB" -> ColumnType(1, 2)), partitionSize))
    val partition2 = system.actorOf(Partition.props(1, Map("columnA" -> ColumnType(3, 4), "columnB" -> ColumnType(3, 4)), partitionSize))
    val table = system.actorOf(Table.propsWithPartitions(Seq("columnA", "columnB"), Map(1 -> partition1, 2 -> partition2)))

    val operator = system.actorOf(ProjectionOperator.props(table, Seq("columnA")))
    operator ! Execute()
    val msg = expectMsgType[QueryResult]

    checkTable(msg.resultTable, Map(
      0 -> Map("columnA" -> ColumnType(1, 2)),
      1 -> Map("columnA" -> ColumnType(3, 4))))
  }

  "A ProjectionOperator projecting multiple column" should "handle multiple partitions" in {
    val table = generateTableTestProbe(Seq(
      Map("a" -> ColumnType(1, 2, 3), "b" -> ColumnType(1, 2, 3)),
      Map("a" -> ColumnType(4, 5, 6), "b" -> ColumnType(4, 5, 6)),
      Map("a" -> ColumnType(7, 8, 9), "b" -> ColumnType(7, 8, 9))))

    val projectionOperator = system.actorOf(ProjectionOperator.props(table, Seq("a", "b")))

    projectionOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Map(
      0 -> Map("a" -> ColumnType(1, 2, 3), "b" -> ColumnType(1, 2, 3)),
      1 -> Map("a" -> ColumnType(4, 5, 6), "b" -> ColumnType(4, 5, 6)),
      2 -> Map("a" -> ColumnType(7, 8, 9), "b" -> ColumnType(7, 8, 9))))
  }
}
