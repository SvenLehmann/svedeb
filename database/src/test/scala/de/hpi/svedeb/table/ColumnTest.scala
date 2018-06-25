package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.table.Column._
import org.scalatest.Matchers._

class ColumnTest extends AbstractActorTest("ColumnTest") {

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! FilterColumn(_ => true)
    expectMsg(FilteredRowIndices(0, "SomeColumnName", Seq.empty[Int]))
  }

  it should "be initialized with values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName", ColumnType(1, 2)))
    column ! ScanColumn()
    val scannedValues = expectMsgType[ScannedValues]
    scannedValues.values.size() shouldEqual 2
    scannedValues.values shouldEqual ColumnType(1, 2)
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))

    column ! AppendValue(3)
    expectMsg(ValueAppended(0, "SomeColumnName"))
  }

  it should "return all indices on wildcard" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue(1)
    column ! AppendValue(2)
    column ! AppendValue(3)
    column ! FilterColumn(_ => true)

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(FilteredRowIndices(0, "SomeColumnName", Seq(0, 1, 2)))
  }

  it should "filter its values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue(1)
    column ! AppendValue(2)
    column ! AppendValue(3)
    column ! FilterColumn(_ == 2)

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(FilteredRowIndices(0, "SomeColumnName", Seq(1)))
  }

  it should "scan its values with indices" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue(1)
    column ! AppendValue(2)
    column ! AppendValue(3)
    column ! ScanColumn(Some(Seq(1, 2)))

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ScannedValues(0, "SomeColumnName", ColumnType(2, 3)))
  }

  it should "scan its values without indices" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue(1)
    column ! AppendValue(2)
    column ! AppendValue(3)
    column ! ScanColumn()

    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ScannedValues(0, "SomeColumnName", ColumnType(1, 2, 3)))
  }

  it should "return column size" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue(1)
    column ! AppendValue(2)
    column ! AppendValue(3)

    column ! GetColumnSize()
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ValueAppended(0, "SomeColumnName"))
    expectMsg(ColumnSize(0, 3))
  }
}
