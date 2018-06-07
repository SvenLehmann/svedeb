package de.hpi.svedeb.operators

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
import org.scalatest.Matchers._

class ScanOperatorTest extends AbstractActorTest("ScanOperator") {

  "A ScanOperator actor" should "scan whole table" in {
    val table = generateTableTestProbe(Seq(
      Map("a" -> ColumnType(1, 2, 3), "b" -> ColumnType(1, 2, 3))))

    val scanOperator = system.actorOf(ScanOperator.props(table, "a", _ => true))
    scanOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Map(0 -> Map("a" -> ColumnType(1, 2, 3), "b" -> ColumnType(1, 2, 3))))
  }

  it should "filter values without test probes" in {
    val partitionSize = 2
    val partition1 = system.actorOf(Partition.props(1,
      Map("columnA" -> ColumnType(1, 2), "columnB" -> ColumnType(1, 2)), partitionSize))
    val partition2 = system.actorOf(Partition.props(2,
      Map("columnA" -> ColumnType(3, 4), "columnB" -> ColumnType(3, 4)), partitionSize))
    val table = system.actorOf(Table.propsWithPartitions(Seq("columnA", "columnB"), Map(1 -> partition1, 2 -> partition2)))
    val operator = system.actorOf(ScanOperator.props(table, "columnA", _ == 1))

    operator ! Execute()
    val msg = expectMsgType[QueryResult]

    checkTable(msg.resultTable, Map(
      1 -> Map("columnA" -> ColumnType(1), "columnB" -> ColumnType(1))))
  }
}
