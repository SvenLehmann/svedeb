package de.hpi.svedeb.operators

import akka.actor.ActorRef
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
;

class EndToEndOperatorTests extends AbstractActorTest("EndToEndTest") {

  private def setupTable(partitionSize: Int, table: Seq[Map[String, ColumnType]]): ActorRef = {
    val partitions = table.zipWithIndex.map {
      case (partition, id) => (id, system.actorOf(Partition.props(id, partition, partitionSize)))
    }.toMap
    system.actorOf(Table.propsWithPartitions(table.headOption.getOrElse(Map()).keys.toSeq, partitions))
  }

  "A query" should "work with chained operators" in {
    val partitionSize = 2
    val content = Seq(
      Map("columnA" -> ColumnType(1, 2), "columnB" -> ColumnType(1, 2)),
      Map("columnA" -> ColumnType(3, 4), "columnB" -> ColumnType(3, 4))
    )
    val table = setupTable(partitionSize, content)

    val scanOperator = system.actorOf(ScanOperator.props(table, "columnA", _ == 2))
    scanOperator ! Execute()
    val scanResult = expectMsgType[QueryResult]

    val projectionOperator = system.actorOf(ProjectionOperator.props(scanResult.resultTable, Seq("columnA")))
    projectionOperator ! Execute()
    val projectionResult = expectMsgType[QueryResult]

    checkTable(projectionResult.resultTable, Map(0 -> Map("columnA" -> ColumnType(2))))
  }

}
