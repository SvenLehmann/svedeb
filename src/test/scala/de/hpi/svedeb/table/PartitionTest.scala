package de.hpi.svedeb.table

import akka.actor.Status.Failure
import akka.testkit.{TestKit, TestProbe}
import de.hpi.svedeb.AbstractTest
import de.hpi.svedeb.table.Column.{ColumnName, GetColumnName}
import de.hpi.svedeb.table.Partition._

class PartitionTest extends AbstractTest("PartitionTest") {

  "A partition actor" should "be initialized with columns" in {
    val column = TestProbe("someColumn")
    val partition = system.actorOf(Partition.props(0, List("someColumn")))

    partition ! ListColumns()
    assert(expectMsgPF() { case m: ColumnList => m.columns.size == 1 })
  }

  it should "return its columns names" in {
    val column = TestProbe("someColumn")
    val partition = system.actorOf(Partition.props(0, List("someColumn")))

    partition ! ListColumns()
    assert(expectMsgPF() { case m: ColumnList => m.columns.size == 1 })
  }

  it should "return a column ref" in {
    val partition = system.actorOf(Partition.props(0, List("someColumn", "someOtherColumn")))

    partition ! GetColumn("someOtherColumn")
    assert(expectMsgPF() { case m: ColumnRetrieved =>
      m.column ! GetColumnName()
      expectMsgPF() { case m: ColumnName => m.name == "someOtherColumn"}
    })
  }

  it should "add a row with one column" in {
    val partition = system.actorOf(Partition.props(0, List("someColumn")))

    partition ! AddRow(RowType(List("someValue")))
    expectMsg(RowAdded())
  }

  it should "add a row with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, List("column1", "column2")))

    partition ! AddRow(RowType(List("value1", "value2")))
    expectMsg(RowAdded())
  }

  it should "add multiple rows with multiple columns" in {
    val partition = system.actorOf(Partition.props(0, List("column1", "column2")))

    partition ! AddRow(RowType(List("value1", "value2")))
    partition ! AddRow(RowType(List("value3", "value4")))
    expectMsg(RowAdded())
    expectMsg(RowAdded())
  }

  it should "return Partition Full" in {
    val partition = system.actorOf(Partition.props(0, List("column1", "column2"), 1))

    partition ! AddRow(RowType(List("value1", "value2")))
    expectMsg(RowAdded())

    partition ! AddRow(RowType(List("value3", "value4")))
    expectMsg(PartitionFull())
  }

  it should "throw an error when row is added that does not match table columns" in {
    val partition = system.actorOf(Partition.props(0))
    partition ! AddRow(RowType(List("someValue")))
    expectMsgType[Failure]
  }

}
