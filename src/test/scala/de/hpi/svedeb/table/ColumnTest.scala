package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column._

class ColumnTest extends AbstractActorTest("ColumnTest") {

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! Filter(_ => true)
    expectMsg(FilteredIndizes(Seq.empty[Int]))
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended())
  }

  it should "filter its values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! Filter(_ => true)

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(FilteredIndizes(Seq(0, 1, 2)))
  }

  it should "scan its values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! Scan(Some(Seq(1, 2)))

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ScannedValues(ColumnType(IndexedSeq("value2", "value3"))))
  }

  it should "scan its values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! Scan(None)

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ScannedValues(ColumnType(IndexedSeq("value1", "value2", "value3"))))
  }

  it should "return column size" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")

    column ! GetNumberOfRows()
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(NumberOfRows(3))
  }
}
