package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Table._
import org.scalatest.Matchers._

class TableTest extends AbstractActorTest("TableTest") {

  "A new table actor" should "store columns" in {
    val table = system.actorOf(Table.props(Seq("a", "b")))
    table ! ListColumnsInTable()
    expectMsg(ColumnList(Seq("a", "b")))
  }

  it should "not contain partitions" in {
    val table = system.actorOf(Table.props(Seq("a")))
    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual 0
  }

  it should "retrieve columns" in {
    val table = system.actorOf(Table.props(Seq("a")))
    table ! GetColumnFromTable("a")
    val actorsForColumn = expectMsgType[ActorsForColumn]
    actorsForColumn.columnActors.size shouldEqual  0
  }

  it should "add a row" in {
    val table = system.actorOf(Table.props(Seq("a", "b")), "table")
    table ! AddRowToTable(RowType(1, 2))
    expectMsgType[RowAddedToTable]

    checkTable(table, Map(0 -> Map("a" -> ColumnType(1), "b" -> ColumnType(2))))
  }

  it should "create a new partition if existing ones are full" in {
    val table = system.actorOf(Table.props(Seq("a"), 2))
    table ! AddRowToTable(RowType(1))
    expectMsgType[RowAddedToTable]

    table ! AddRowToTable(RowType(2))
    expectMsgType[RowAddedToTable]

    table ! AddRowToTable(RowType(3))
    expectMsgType[RowAddedToTable]

    checkTable(table, Map(0 -> Map("a" -> ColumnType(1, 2)), 1 -> Map("a" -> ColumnType(3))))
  }

  it should "fail to add wrong row definition" in {
    val table = system.actorOf(Table.props(Seq("a"), 2))
    table ! AddRowToTable(RowType(1, 2))
  }

  "A table with multiple partitions" should "insert rows correctly aligned" in {
    val numberOfRows = 10
    val partitionSize = 1
    val table = system.actorOf(Table.props(Seq("a", "b", "c", "d"), partitionSize))

    (0 until numberOfRows).foreach(id => table ! AddRowToTable(RowType(id, id, id, id)))
    (0 until numberOfRows).foreach(_ => expectMsg(RowAddedToTable()))

    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual numberOfRows

    val materializedTable = partitions.partitions.mapValues(p => materializePartition(p))
    materializedTable.foreach {
      case (id, partition) if id < (materializedTable.size - 1) => partition.values.head.size shouldEqual partitionSize
      case _ => // ignore
    }

    checkTableIgnoreOrder(table, Map(
      0 -> Map("a" -> ColumnType(0), "b" -> ColumnType(0), "c" -> ColumnType(0), "d" -> ColumnType(0)),
      1 -> Map("a" -> ColumnType(1), "b" -> ColumnType(1), "c" -> ColumnType(1), "d" -> ColumnType(1)),
      2 -> Map("a" -> ColumnType(2), "b" -> ColumnType(2), "c" -> ColumnType(2), "d" -> ColumnType(2)),
      3 -> Map("a" -> ColumnType(3), "b" -> ColumnType(3), "c" -> ColumnType(3), "d" -> ColumnType(3)),
      4 -> Map("a" -> ColumnType(4), "b" -> ColumnType(4), "c" -> ColumnType(4), "d" -> ColumnType(4)),
      5 -> Map("a" -> ColumnType(5), "b" -> ColumnType(5), "c" -> ColumnType(5), "d" -> ColumnType(5)),
      6 -> Map("a" -> ColumnType(6), "b" -> ColumnType(6), "c" -> ColumnType(6), "d" -> ColumnType(6)),
      7 -> Map("a" -> ColumnType(7), "b" -> ColumnType(7), "c" -> ColumnType(7), "d" -> ColumnType(7)),
      8 -> Map("a" -> ColumnType(8), "b" -> ColumnType(8), "c" -> ColumnType(8), "d" -> ColumnType(8)),
      9 -> Map("a" -> ColumnType(9), "b" -> ColumnType(9), "c" -> ColumnType(9), "d" -> ColumnType(9))
    ))
  }

  it should "insert rows correctly aligned (2)" in {
    val numberOfRows = 10
    val partitionSize = 2
    val table = system.actorOf(Table.props(Seq("a", "b", "c", "d"), partitionSize))

    (0 until numberOfRows).foreach(id => table ! AddRowToTable(RowType(id, id, id, id)))
    (0 until numberOfRows).foreach(_ => expectMsg(RowAddedToTable()))

    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
    partitions.partitions.size shouldEqual (numberOfRows / 2)

    val materializedTable = partitions.partitions.mapValues(p => materializePartition(p))
    materializedTable.foreach {
      case (id, partition) if id < (materializedTable.size - 1) => partition.values.head.size shouldEqual partitionSize
      case _ => // ignore
    }

    checkTableIgnoreOrder(table, Map(
      0 -> Map("a" -> ColumnType(0, 1), "b" -> ColumnType(0, 1), "c" -> ColumnType(0, 1), "d" -> ColumnType(0, 1)),
      1 -> Map("a" -> ColumnType(2, 3), "b" -> ColumnType(2, 3), "c" -> ColumnType(2, 3), "d" -> ColumnType(2, 3)),
      2 -> Map("a" -> ColumnType(4, 5), "b" -> ColumnType(4, 5), "c" -> ColumnType(4, 5), "d" -> ColumnType(4, 5)),
      3 -> Map("a" -> ColumnType(6, 7), "b" -> ColumnType(6, 7), "c" -> ColumnType(6, 7), "d" -> ColumnType(6, 7)),
      4 -> Map("a" -> ColumnType(8, 9), "b" -> ColumnType(8, 9), "c" -> ColumnType(8, 9), "d" -> ColumnType(8, 9))
    ))
  }

  it should "insert rows correctly aligned (3)" in {
    val numberOfRows = 10
    val partitionSize = 3
    val table = system.actorOf(Table.props(Seq("a", "b", "c", "d"), partitionSize))

    (0 until numberOfRows).foreach(id => table ! AddRowToTable(RowType(id, id, id, id)))
    (0 until numberOfRows).foreach(_ => expectMsg(RowAddedToTable()))

    table ! GetPartitions()
    val partitions = expectMsgType[PartitionsInTable]
//    partitions.partitions.size shouldEqual (numberOfRows / 3.0).ceil.toInt

    val materializedTable = partitions.partitions.mapValues(p => materializePartition(p))

    val foo = materializedTable.mapValues(_.mapValues(_.size()))
    println(foo)


    materializedTable.foreach {
      case (id, partition) if id < (materializedTable.size - 1) => partition.values.head.size shouldEqual partitionSize
      case _ => // ignore
    }



    checkTableIgnoreOrder(table, Map(
      0 -> Map("a" -> ColumnType(0, 1, 2), "b" -> ColumnType(0, 1, 2), "c" -> ColumnType(0, 1, 2), "d" -> ColumnType(0, 1, 2)),
      1 -> Map("a" -> ColumnType(3, 4, 5), "b" -> ColumnType(3, 4, 5), "c" -> ColumnType(3, 4, 5), "d" -> ColumnType(3, 4, 5)),
      2 -> Map("a" -> ColumnType(6, 7, 8), "b" -> ColumnType(6, 7, 8), "c" -> ColumnType(6, 7, 8), "d" -> ColumnType(6, 7, 8)),
      3 -> Map("a" -> ColumnType(9), "b" -> ColumnType(9), "c" -> ColumnType(9), "d" -> ColumnType(9))
    ))
  }
}
