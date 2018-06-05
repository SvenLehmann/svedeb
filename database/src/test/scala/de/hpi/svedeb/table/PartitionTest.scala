package de.hpi.svedeb.table

import akka.actor.ActorRef
import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column.{ColumnName, GetColumnName}
import de.hpi.svedeb.table.Partition._
import org.scalatest.Matchers._

class PartitionTest extends AbstractActorTest("PartitionTest") {

  "A partition actor" should "be initialized with columns" in {
    val column = TestProbe("SomeColumn")
    val partition = system.actorOf(Partition.props(0, Seq("someColumn")))

    partition ! ListColumnNames()
    val columnNameList = expectMsgType[ColumnNameList]
    columnNameList.columns shouldEqual Seq("someColumn")
  }

  it should "be initialized with values" in {
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType(1)), 10))

    checkPartition(partition, Map("someColumn" -> ColumnType(1)))
  }

  it should "return a column ref" in {
    val partition = system.actorOf(Partition.props(0, Seq("someColumn", "someOtherColumn")))

    partition ! GetColumn("someOtherColumn")
    val columnRetrieved = expectMsgType[ColumnRetrieved]
    columnRetrieved.column ! GetColumnName()
    val columnName = expectMsgType[ColumnName]
    columnName.name shouldEqual "someOtherColumn"
  }

  it should "add a row with one column" in {
    val partition = system.actorOf(Partition.props(0, Seq("someColumn")))

    partition ! AddRow(RowType(1), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))
  }

  it should "add a row with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2")))

    partition ! AddRow(RowType(1, 2), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))
  }

  it should "add multiple rows with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2")))

    partition ! AddRow(RowType(1, 2), ActorRef.noSender)
    partition ! AddRow(RowType(3, 4), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))
    expectMsg(RowAdded(ActorRef.noSender))

    val expectedPartition = Map("column1" -> ColumnType(1, 3), "column2" -> ColumnType(2, 4))
    checkPartition(partition, expectedPartition)
  }

  it should "return Partition Full" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2"), 1))

    partition ! AddRow(RowType(1, 2), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))

    val row = RowType(3, 4)
    partition ! AddRow(row, ActorRef.noSender)
    expectMsg(PartitionFull(row, ActorRef.noSender))

    checkPartition(partition, Map("column1" -> ColumnType(1), "column2" -> ColumnType(2)))
  }
}
