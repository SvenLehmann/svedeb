package de.hpi.svedeb.management

import akka.actor.Status.Failure
import de.hpi.svedeb.AbstractTest
import de.hpi.svedeb.management.TableManager._

class TableManagerTest extends AbstractTest("PartitionTest") {

  "A new TableManager" should "not contain tables" in {
    val tableManager = system.actorOf(TableManager.props())
    tableManager ! ListTables()

    expectMsg(TableList(List.empty[String]))
  }

  it should "add a table" in {
    val tableManager = system.actorOf(TableManager.props())
    tableManager ! AddTable("SomeTable", List("columnA", "columnB"))
    expectMsgType[TableAdded]

    tableManager ! ListTables()
    expectMsg(TableList(List("SomeTable")))
  }

  it should "drop a table" in {
    val tableManager = system.actorOf(TableManager.props())

    // Non-existing tables can be removed as well..
    tableManager ! RemoveTable("SomeTable")
    expectMsgType[TableRemoved]

    tableManager ! AddTable("SomeTable", List("columnA", "columnB"))
    expectMsgType[TableAdded]

    tableManager ! RemoveTable("SomeTable")
    expectMsgType[TableRemoved]
  }

  it should "fetch a table" in {
    val tableManager = system.actorOf(TableManager.props())
    tableManager ! AddTable("SomeTable", List("columnA", "columnB"))
    expectMsgType[TableAdded]

    tableManager ! FetchTable("SomeTable")
    expectMsgType[TableFetched]

    tableManager ! FetchTable("Non-existing table")
    expectMsgType[Failure]
  }
}
