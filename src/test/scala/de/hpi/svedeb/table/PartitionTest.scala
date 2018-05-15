package de.hpi.svedeb.table

import akka.actor.ActorRef
import akka.actor.Status.Failure
import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column.{ColumnName, GetColumnName, ScanColumn, ScannedValues}
import de.hpi.svedeb.table.Partition._
import org.scalatest.Matchers._

class PartitionTest extends AbstractActorTest("PartitionTest") {

  "A partition actor" should "be initialized with columns" in {
    val column = TestProbe("SomeColumn")
    val partition = system.actorOf(Partition.props(0, Seq("someColumn")))

    partition ! ListColumnNames()
    assert(expectMsgPF() { case m: ColumnNameList => m.columns.size == 1 })
  }

  it should "be initialized with values" in {
    val partition = system.actorOf(Partition.props(0, Map("someColumn" -> ColumnType("a")), 10))

    partition ! GetColumns()
    val nameList = expectMsgType[ColumnsRetrieved]
    nameList.columns.size shouldEqual 1

    nameList.columns.foreach{ case (name, actorRef) => actorRef ! ScanColumn()}
    assert(expectMsgPF() { case m: ScannedValues => m.values == ColumnType("a") })
  }

  it should "return its columns names" in {
    val column = TestProbe("SomeColumn")
    val partition = system.actorOf(Partition.props(0, Seq("someColumn")))

    partition ! ListColumnNames()
    assert(expectMsgPF() { case m: ColumnNameList => m.columns.size == 1 })
  }

  it should "return a column ref" in {
    val partition = system.actorOf(Partition.props(0, Seq("someColumn", "someOtherColumn")))

    partition ! GetColumn("someOtherColumn")
    assert(expectMsgPF() { case m: ColumnRetrieved =>
      m.column ! GetColumnName()
      expectMsgPF() { case m: ColumnName => m.name == "someOtherColumn"}
    })
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
  }

  it should "return Partition Full" in {
    val partition = system.actorOf(Partition.props(0, Seq("column1", "column2"), 1))

    partition ! AddRow(RowType("value1", "value2"), ActorRef.noSender)
    expectMsg(RowAdded(ActorRef.noSender))

    val row = RowType("value3", "value4")
    partition ! AddRow(row, ActorRef.noSender)
    expectMsg(PartitionFull(row, ActorRef.noSender))
  }

  it should "throw an error when row is added that does not match table columns" in {
    val partition = system.actorOf(Partition.props(0))
    partition ! AddRow(RowType("someValue"), ActorRef.noSender)
    expectMsgType[Failure]
  }

}
