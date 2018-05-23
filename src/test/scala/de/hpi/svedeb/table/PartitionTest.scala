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
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType("a")), 10))

    checkPartition(partition, Map("someColumn" -> ColumnType("a")))
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

    partition ! AddRow(RowType("someValue"), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))
  }

  it should "add a row with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2")))

    partition ! AddRow(RowType("value1", "value2"), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))
  }

  it should "add multiple rows with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2")))

    partition ! AddRow(RowType("value1", "value2"), ActorRef.noSender)
    partition ! AddRow(RowType("value3", "value4"), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))
    expectMsg(RowAdded(ActorRef.noSender))

    checkPartition(partition, Map("column1" -> ColumnType("value1", "value3"), "column2" -> ColumnType("value2", "value4")))
  }

  it should "return Partition Full" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2"), 1))

    partition ! AddRow(RowType("value1", "value2"), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))

    val row = RowType("value3", "value4")
    partition ! AddRow(row, ActorRef.noSender)
    expectMsg(PartitionFull(row, ActorRef.noSender))

    checkPartition(partition, Map("column1" -> ColumnType("value1"), "column2" -> ColumnType("value2")))
  }
}
