package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Table._
import de.hpi.svedeb.utils.Utils.ValueType
import org.scalatest.Matchers._

// TODO: Add Table test helper to create table from raw data
class TableTest extends AbstractActorTest("TableTest") {

  "A new table actor" should "store columns" in {
    val table = system.actorOf(Table.props(Seq("columnA", "columnB")))
    table ! ListColumnsInTable()
    expectMsg(ColumnList(Seq("columnA", "columnB")))
  }

  it should "not contain partitions" in {
    val table = system.actorOf(Table.props(Seq("columnA")))
    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual 0
  }

  it should "retrieve columns" in {
    val table = system.actorOf(Table.props(Seq("columnA")))
    table ! GetColumnFromTable("columnA")
    val actorsForColumn = expectMsgType[ActorsForColumn]
    actorsForColumn.columnActors.size shouldEqual  0
  }

  it should "add a row" in {
    val table = system.actorOf(Table.props(Seq("columnA", "columnB")), "table")
    table ! AddRowToTable(RowType(1, 2))
    expectMsg(RowAddedToTable())
  }

  it should "create a new partition if existing ones are full" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 2))
    table ! AddRowToTable(RowType(1))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(RowType(2))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(RowType(3))
    expectMsg(RowAddedToTable())

    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual 2

    table ! GetColumnFromTable("columnA")
    val actorsForColumn = expectMsgType[ActorsForColumn]
    actorsForColumn.columnActors.size shouldEqual  2
  }

  it should "fail to add wrong row definition" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 2))
    table ! AddRowToTable(RowType(1, 2))
  }

  "A table with multiple partitions" should "insert rows correctly aligned" in {
    val numberOfPartitions = 10
    val orderTable = system.actorOf(Table.props(Seq("columnA", "columnB", "columnC", "columnD"), 1), "orderTable")

    (0 until numberOfPartitions).foreach(id => orderTable ! AddRowToTable(RowType(id, id, id, id)))
    (0 until numberOfPartitions).foreach(_ => expectMsg(RowAddedToTable()))

    orderTable ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual numberOfPartitions

    def checkColumnValues(suffix: String): Seq[ValueType] = {
      orderTable ! GetColumnFromTable(s"column${suffix.toUpperCase}")
      val columnActors = expectMsgType[ActorsForColumn]
      columnActors.columnActors.size shouldEqual numberOfPartitions

      columnActors.columnActors.values.foreach(columnActor => columnActor ! ScanColumn())
      val values = (1 to numberOfPartitions)
        .map(_ => expectMsgType[ScannedValues])
        .sortBy(c => c.partitionId)
        .flatMap(c => c.values.values)

      // Verify correct values
      values.sorted shouldEqual (0 until numberOfPartitions).toVector.sorted
      values
    }

    val valuesA = checkColumnValues("a")
    val valuesB = checkColumnValues("b")
    val valuesC = checkColumnValues("c")
    val valuesD = checkColumnValues("d")

    valuesA shouldEqual valuesB
    valuesA shouldEqual valuesC
    valuesA shouldEqual valuesD

  }
}
