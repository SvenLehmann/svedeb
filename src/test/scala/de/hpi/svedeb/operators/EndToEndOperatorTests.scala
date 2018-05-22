package de.hpi.svedeb.operators

import akka.actor.ActorRef
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.table.{ColumnType, Partition, Table}
;

class EndToEndOperatorTests extends AbstractActorTest("EndToEndTest") {

  private def setupTable(partitionSize: Int, table: Seq[Map[String, ColumnType]]): ActorRef = {
    val partitions = table.zipWithIndex.map { case (partition, id) => system.actorOf(Partition.props(id, partition, partitionSize)) }
    system.actorOf(Table.props(table.headOption.getOrElse(Map()).keys.toSeq, partitionSize, partitions))
  }

  "A query" should "work with chained operators" in {
    val partitionSize = 2
    val content = Seq(
      Map("columnA" -> ColumnType("a1", "a2"), "columnB" -> ColumnType("b1", "b2")),
      Map("columnA" -> ColumnType("a3", "a4"), "columnB" -> ColumnType("b3", "b4"))
    )
    val table = setupTable(partitionSize, content)

    val scanOperator = system.actorOf(ScanOperator.props(table, "columnA", _ == "a2"))
    scanOperator ! Execute()
    val scanResult = expectMsgType[QueryResult]

    val projectionOperator = system.actorOf(ProjectionOperator.props(scanResult.resultTable, Seq("columnA")))
    projectionOperator ! Execute()
    val projectionResult = expectMsgType[QueryResult]

    checkTable(projectionResult.resultTable, Seq(Map("columnA" -> ColumnType("a2"))))
  }

}
