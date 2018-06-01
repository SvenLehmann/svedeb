package de.hpi.svedeb.operators

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
import org.scalatest.Matchers._

// TODO: Consider splitting up this test into multiple smaller ones that do not have so many dependencies
class ScanOperatorTest extends AbstractActorTest("ScanOperator") {

  "A ScanOperator actor" should "scan whole table" in {
    val table = generateTableTestProbe(Seq(
      Map("a" -> ColumnType("1", "2", "3"), "b" -> ColumnType("1", "2", "3"))))

    val scanOperator = system.actorOf(ScanOperator.props(table, "a", _ => true))
    scanOperator ! Execute()
    val operatorResult = expectMsgType[QueryResult]

    checkTable(operatorResult.resultTable, Seq(Map("a" -> ColumnType("1", "2", "3"), "b" -> ColumnType("1", "2", "3"))))
  }

  it should "filter values without test probes" in {
    val partitionSize = 2
    val partition1 = system.actorOf(Partition.props(0, Map("columnA" -> ColumnType("a1", "a2"), "columnB" -> ColumnType("b1", "b2")), partitionSize))
    val partition2 = system.actorOf(Partition.props(1, Map("columnA" -> ColumnType("a3", "a4"), "columnB" -> ColumnType("b3", "b4")), partitionSize))
    val table = system.actorOf(Table.props(Seq("columnA", "columnB"), partitionSize, Seq(partition1, partition2)))
    val operator = system.actorOf(ScanOperator.props(table, "columnA", x => x.contains("1")))

    operator ! Execute()
    val msg = expectMsgType[QueryResult]

    checkTable(msg.resultTable, Seq(
      Map("columnA" -> ColumnType("a1"), "columnB" -> ColumnType("b1"))))
  }
}
