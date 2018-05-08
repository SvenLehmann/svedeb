package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column._

class ColumnTest extends AbstractActorTest("ColumnTest") {

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! FilterColumn(_ => true)
    expectMsg(FilteredRowIndizes(Seq.empty[Int]))
  }

  it should "be initialized with values" in {
    val column = system.actorOf(Column.props("SomeColumnName", ColumnType("a", "b")))
    column ! ScanColumn(None)
    expectMsgPF(){ case m: ScannedValues => m.values.size() == 2 && m.values == Seq("a", "b")}
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props("SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended())
  }

  it should "filter its values" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! FilterColumn(_ => true)

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(FilteredRowIndizes(Seq(0, 1, 2)))
  }

  it should "scan its values with indizes" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! ScanColumn(Some(Seq(1, 2)))

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ScannedValues("SomeColumnName", ColumnType("value2", "value3")))
  }

  it should "scan its values without indizes" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! ScanColumn(None)

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ScannedValues("SomeColumnName", ColumnType("value1", "value2", "value3")))
  }

  it should "return column size" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")

    column ! GetColumnSize()
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ColumnSize(3))
  }
}
