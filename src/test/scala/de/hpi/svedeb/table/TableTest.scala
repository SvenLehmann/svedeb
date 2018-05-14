package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

// TODO: Add Table test helper to create table from raw data
class TableTest extends AbstractActorTest("TableTest") {

  "A new table actor" should "store columns" in {
    val table = system.actorOf(Table.props(Seq("columnA", "columnB"), 10))
    table ! ListColumnsInTable()
    expectMsg(ColumnList(Seq("columnA", "columnB")))
  }

  it should "retrieve partition" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 10))
    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 })
  }

  it should "retrieve columns" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 10))
    table ! GetColumnFromTable("columnA")
    assert(expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 1 })
  }

  it should "add a row" in {
    val table = system.actorOf(Table.props(Seq("columnA", "columnB"), 10), "table")
    table ! AddRowToTable(RowType("valueA", "valueB"))
    expectMsg(RowAddedToTable())
  }

  it should "create a new partition if existing ones are full" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 2))
    table ! AddRowToTable(RowType("value1"))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(RowType("value2"))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(RowType("value3"))
    expectMsg(RowAddedToTable())

    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 2 })

    table ! GetColumnFromTable("columnA")
    assert(expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 2 })
  }

  it should "fail to add wrong row definition" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 2))
    table ! AddRowToTable(RowType("value1", "value2"))
  }

  "A table with multiple partitions" should "insert rows correctly aligned" in {
    val numberOfPartitions = 10
    val orderTable = system.actorOf(Table.props(Seq("columnA", "columnB", "columnC", "columnD"), 1), "orderTable")

    (1 to numberOfPartitions).foreach(rowId => orderTable ! AddRowToTable(RowType("a" + rowId, "b" + rowId, "c" + rowId, "d" + rowId)))
    (1 to numberOfPartitions).foreach(_ => expectMsg(RowAddedToTable()))

    orderTable ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual numberOfPartitions

    orderTable ! GetColumnFromTable("columnA")
    val columnActors = expectMsgType[ActorsForColumn]
    columnActors.columnActors.size shouldEqual numberOfPartitions

    columnActors.columnActors.foreach(columnActor => columnActor ! ScanColumn())
    val valuesA = (1 to numberOfPartitions).map(_ => expectMsgType[ScannedValues]).flatMap(m => m.values.values)
    valuesA.sorted shouldEqual Vector("a1", "a10", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9")
  }
}
