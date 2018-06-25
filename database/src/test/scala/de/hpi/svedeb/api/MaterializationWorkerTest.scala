package de.hpi.svedeb.api

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.api.MaterializationWorker.{MaterializeTable, MaterializedTable}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import de.hpi.svedeb.table.Table.{ActorsForColumn, ColumnList, GetColumnFromTable, ListColumnsInTable}
import org.scalatest.Matchers._

class MaterializationWorkerTest extends AbstractActorTest("MaterializationWorker") {

  "A MaterializationWorker" should "materialize a simple table" in {

    val data = Map("columnA" -> ColumnType(1, 2, 3), "columnB" -> ColumnType(3, 2, 1))
    val table = generateTableTestProbe(Seq(data))

    val worker = system.actorOf(MaterializationWorker.props(self, ActorRef.noSender), "MaterializationWorker")
    worker ! MaterializeTable(table)

    val result = expectMsgType[MaterializedTable]
    result.user shouldEqual ActorRef.noSender
    result.columns shouldEqual data
  }

  it should "materialize a table with multiple partitions" in {

    val table = generateTableTestProbe(Seq(
      Map("columnA" -> ColumnType(1, 2), "columnB" -> ColumnType(2, 1)),
      Map("columnA" -> ColumnType(3, 4), "columnB" -> ColumnType(6, 7)),
      Map("columnA" -> ColumnType(5, 6, 7), "columnB" -> ColumnType(5, 4, 3))
    ))

    val worker = system.actorOf(MaterializationWorker.props(self, ActorRef.noSender))
    worker ! MaterializeTable(table)

    val result = expectMsgType[MaterializedTable]
    result.user shouldEqual ActorRef.noSender
    val valuesA = ColumnType(1, 2, 3, 4, 5, 6, 7)
    val valuesB = ColumnType(2, 1, 6, 7, 5, 4, 3)
    result.columns shouldEqual Map("columnA" -> valuesA, "columnB" -> valuesB)
  }

}
