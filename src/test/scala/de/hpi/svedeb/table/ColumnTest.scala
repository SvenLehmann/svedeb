package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column._

class ColumnTest extends AbstractActorTest("ColumnTest") {

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! FilterColumn(_ => true)
    expectMsg(FilteredRowIndizes(0, "SomeColumnName", Seq.empty[Int]))
  }

  it should "be initialized with values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName", ColumnType("a", "b")))
    column ! ScanColumn()
    expectMsgPF(){ case m: ScannedValues => m.values.size() == 2 && m.values == Seq("a", "b")}
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended(0, "SomeColumnName"))
  }

  it should "filter its values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! FilterColumn(_ => true)

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(FilteredRowIndizes(0, "SomeColumnName", Seq(0, 1, 2)))
  }

  it should "scan its values with indizes" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! ScanColumn(Some(Seq(1, 2)))

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ScannedValues(0, "SomeColumnName", ColumnType("value2", "value3")))
  }

  it should "scan its values without indizes" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! ScanColumn()

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ScannedValues(0, "SomeColumnName", ColumnType("value1", "value2", "value3")))
  }

  it should "return column size" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")

    column ! GetColumnSize()
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ColumnSize(0, 3))
  }
}
