package de.hpi.svedeb.table

import akka.actor.ActorRef
import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column.{ColumnName, GetColumnName}
import de.hpi.svedeb.table.Partition._
import org.scalatest.Matchers._

class PartitionTest extends AbstractActorTest("PartitionTest") {

  "A partition actor" should "be initialized with columns" in {
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType())))

    partition ! ListColumnNames()
    val columnNameList = expectMsgType[ColumnNameList]
    columnNameList.columns shouldEqual Seq("someColumn")
  }

  it should "be initialized with values" in {
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType(1)), 10))

    checkPartition(partition, Map("someColumn" -> ColumnType(1)))
  }

  it should "return a column ref" in {
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType(), "someOtherColumn" -> ColumnType())))

    partition ! GetColumn("someOtherColumn")
    val columnRetrieved = expectMsgType[ColumnRetrieved]
    columnRetrieved.column ! GetColumnName()
    val columnName = expectMsgType[ColumnName]
    columnName.name shouldEqual "someOtherColumn"
  }

  it should "add a row with one column" in {
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType())))

    partition ! AddRow(RowType(1), ActorRef.noSender)
    expectMsgType[RowAdded]

    checkPartition(partition, Map("someColumn" -> ColumnType(1)))
  }

  it should "add a row with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, Map("column1" -> ColumnType(), "column2" -> ColumnType())))

    partition ! AddRow(RowType(1, 2), ActorRef.noSender)
    expectMsgType[RowAdded]

    checkPartition(partition, Map("column1" -> ColumnType(1), "column2" -> ColumnType(2)))
  }

  it should "add multiple rows with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, Map("column1" -> ColumnType(), "column2" -> ColumnType())))

    partition ! AddRow(RowType(1, 2), ActorRef.noSender)
    partition ! AddRow(RowType(3, 4), ActorRef.noSender)
    expectMsgType[RowAdded]
    expectMsgType[RowAdded]

    val expectedPartition = Map("column1" -> ColumnType(1, 3), "column2" -> ColumnType(2, 4))
    checkPartition(partition, expectedPartition)
  }

  it should "return Partition Full" in {
    val partition = system.actorOf(Partition.props(0, Map("column1" -> ColumnType(), "column2" -> ColumnType()), 1))

    partition ! AddRow(RowType(1, 2), ActorRef.noSender)
    expectMsgType[RowAdded]

    val row = RowType(3, 4)
    partition ! AddRow(row, ActorRef.noSender)
    expectMsgType[PartitionFull]

    // Order of inserted rows is only guaranteed due to blocking 'expectMsgType' calls.
    checkPartition(partition, Map("column1" -> ColumnType(1), "column2" -> ColumnType(2)))
  }

  it should "return PartitionFull (2)" in {
    val partitionSize = 2
    val partition = system.actorOf(Partition.props(0, Map("column1" -> ColumnType(), "column2" -> ColumnType()), partitionSize))

    partition ! AddRow(RowType(1, 1), ActorRef.noSender)
    partition ! AddRow(RowType(2, 2), ActorRef.noSender)
    partition ! AddRow(RowType(3, 3), ActorRef.noSender)
    partition ! AddRow(RowType(4, 4), ActorRef.noSender)

    expectMsgType[RowAdded]
    expectMsgType[RowAdded]
    expectMsgType[PartitionFull]
    expectMsgType[PartitionFull]

    val actualPartition = materializePartition(partition)
    actualPartition.foreach { case (_, column) => column.size() shouldEqual partitionSize }
  }
}
