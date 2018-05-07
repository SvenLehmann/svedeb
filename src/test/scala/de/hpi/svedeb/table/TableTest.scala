package de.hpi.svedeb.table

import akka.testkit.TestKit
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Table._

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
    val table = system.actorOf(Table.props(Seq("columnA", "columnB"), 10))
    table ! AddRowToTable(RowType(Seq("valueA", "valueB")))
    expectMsg(RowAddedToTable())
  }

  it should "create a new partition if existing ones are full" in {
    print("executing test")
    val table = system.actorOf(Table.props(Seq("columnA"), 2))
    table ! AddRowToTable(RowType(Seq("value1")))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(RowType(Seq("value2")))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(RowType(Seq("value3")))
    expectMsg(RowAddedToTable())

    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 2 })

    table ! GetColumnFromTable("columnA")
    assert(expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 2 })
  }

  it should "fail to add wrong row definition" in {
    val table = system.actorOf(Table.props(Seq("columnA"), 2))
    table ! AddRowToTable(RowType(Seq("value1", "value2")))

  }
}
