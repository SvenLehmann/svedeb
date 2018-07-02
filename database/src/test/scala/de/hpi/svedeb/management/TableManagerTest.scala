package de.hpi.svedeb.management

import akka.actor.Status.Failure
import akka.testkit.TestProbe
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.management.TableManager._
import org.scalatest.Matchers._

class TableManagerTest extends AbstractActorTest("PartitionTest") {

  "A new TableManager" should "not contain tables" in {
    val tableManager = system.actorOf(TableManager.props())
    tableManager ! ListTables()

    expectMsg(TableList(Seq.empty[String]))
  }

  it should "add a table" in {
    val partition = TestProbe()
    val tableManager = system.actorOf(TableManager.props())
    tableManager ! AddTable("SomeTable", Seq("columnA", "columnB"), Map(0 -> partition.ref))
    expectMsgType[TableAdded]

    tableManager ! ListTables()
    expectMsg(TableList(Seq("SomeTable")))
  }

  it should "drop a table" in {
    val partition = TestProbe()
    val tableManager = system.actorOf(TableManager.props())

    // Non-existing tables can be removed as well..
    tableManager ! RemoveTable("SomeTable")
    expectMsgType[TableRemoved]

    tableManager ! AddTable("SomeTable", Seq("columnA", "columnB"), Map(0 -> partition.ref))
    expectMsgType[TableAdded]

    tableManager ! RemoveTable("SomeTable")
    expectMsgType[TableRemoved]
  }

  it should "fetch a table" in {
    val partition = TestProbe()
    val tableManager = system.actorOf(TableManager.props())
    tableManager ! AddTable("SomeTable", Seq("columnA", "columnB"), Map(0 -> partition.ref))
    expectMsgType[TableAdded]

    tableManager ! FetchTable("SomeTable")
    expectMsgType[TableFetched]

    tableManager ! FetchTable("Non-existing table")
    expectMsgType[Failure]
  }

  it should "list remote table managers" in {
    val remoteTableManager = TestProbe()
    val api = system.actorOf(TableManager.props(Seq(remoteTableManager.ref)))
    api ! ListRemoteTableManagers()
    val remoteTableManagers = expectMsgType[RemoteTableManagers]
    remoteTableManagers.tableManagers shouldEqual Seq(remoteTableManager.ref)
    remoteTableManager.expectMsgType[AddRemoteTableManager]
  }
}
