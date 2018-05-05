package de.hpi.svedeb.table

import akka.testkit.TestKit
import de.hpi.svedeb.AbstractTest
import de.hpi.svedeb.table.Column._

class ColumnTest extends AbstractTest("ColumnTest") {

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! Scan(_ => true)
    expectMsg(ScannedValues(ColumnType(List.empty[String])))
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended())
  }

  it should "expose its values" in {
    val column = system.actorOf(Column.props(0, "SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! Scan(_ => true)

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ScannedValues(ColumnType(List("value1", "value2", "value3"))))
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
