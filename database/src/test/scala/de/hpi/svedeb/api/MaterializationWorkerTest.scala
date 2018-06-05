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

    val data = Map("columnA" -> ColumnType("a", "b", "c"), "columnB" -> ColumnType("c", "b", "a"))
    val table = generateTableTestProbe(Seq(data))

    val worker = system.actorOf(MaterializationWorker.props(self, ActorRef.noSender), "MaterializationWorker")
    worker ! MaterializeTable(table)

    val result = expectMsgType[MaterializedTable]
    result.user shouldEqual ActorRef.noSender
    result.columns shouldEqual data
  }

  it should "materialize a table with multiple partitions" in {

    val table = generateTableTestProbe(Seq(
      Map("columnA" -> ColumnType("a", "b"), "columnB" -> ColumnType("b", "a")),
      Map("columnA" -> ColumnType("c", "d"), "columnB" -> ColumnType("f", "g")),
      Map("columnA" -> ColumnType("e", "f", "g"), "columnB" -> ColumnType("e", "d", "c"))
    ))

    val worker = system.actorOf(MaterializationWorker.props(self, ActorRef.noSender))
    worker ! MaterializeTable(table)

    val result = expectMsgType[MaterializedTable]
    result.user shouldEqual ActorRef.noSender
    val valuesA = ColumnType("a", "b", "c", "d", "e", "f", "g")
    val valuesB = ColumnType("b", "a", "f", "g", "e", "d", "c")
    result.columns shouldEqual Map("columnA" -> valuesA, "columnB" -> valuesB)
  }

}
