package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
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

  it should "return rows in correct order" in {
    val table = system.actorOf(Table.props(Seq("columnA", "columnB", "columnC", "columnD"), 1))

    (1 to 10).foreach(rowId => table ! AddRowToTable(RowType("a" + rowId, "b" + rowId, "c" + rowId, "d" + rowId)))
    (1 to 10).foreach(_ => expectMsg(RowAddedToTable()))

    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual 10

    table ! GetColumnFromTable("columnA")
    val columnActors = expectMsgType[ActorsForColumn]
    columnActors.columnActors.size shouldEqual 10
  }
}
