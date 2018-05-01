package de.hpi.svedeb.table

import akka.testkit.TestKit
import de.hpi.svedeb.AbstractTest
import de.hpi.svedeb.table.Table._

class TableTest extends AbstractTest("TableTest") {

  "A new table actor" should "store columns" in {
    val table = system.actorOf(Table.props(List("columnA", "columnB"), 10))
    table ! ListColumnsInTable()
    expectMsg(ColumnList(List("columnA", "columnB")))
  }

  it should "retrieve partition" in {
    val table = system.actorOf(Table.props(List("columnA"), 10))
    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 })
  }

  it should "retrieve columns" in {
    val table = system.actorOf(Table.props(List("columnA"), 10))
    table ! GetColumnFromTable("columnA")
    assert(expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 1 })
  }

  it should "add a row" in {
    val table = system.actorOf(Table.props(List("columnA", "columnB"), 10))
    table ! AddRowToTable(List("valueA", "valueB"))
    expectMsg(RowAddedToTable())
  }

  it should "create a new partition if existing ones are full" in {
    print("executing test")
    val table = system.actorOf(Table.props(List("columnA"), 2))
    table ! AddRowToTable(List("value1"))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(List("value2"))
    expectMsg(RowAddedToTable())

    table ! AddRowToTable(List("value3"))
    expectMsg(RowAddedToTable())

    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 2 })

    table ! GetColumnFromTable("columnA")
    assert(expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 2 })
  }

  it should "fail to add wrong row definition" in {
    val table = system.actorOf(Table.props(List("columnA"), 2))
    table ! AddRowToTable(List("value1", "value2"))

  }
}
