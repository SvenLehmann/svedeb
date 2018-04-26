package de.hpi.svedeb.table

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.svedeb.table.Column._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, WordSpecLike}

class ColumnTest extends TestKit(ActorSystem("ColumnTest")) with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! Scan
    expectMsg(ScannedValues(List.empty[String]))
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props("SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended)

    column ! Scan
    expectMsg(ScannedValues(List("SomeValue")))
  }

  it should "delete a value" in {
    val column = system.actorOf(Column.props("SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended)

    column ! DeleteValue("SomeValue")
    expectMsg(ValueDeleted)

    column ! Scan
    expectMsg(ScannedValues(List.empty[String]))
  }

  it should "not delete non-existing value" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! DeleteValue("SomeValue")
  }
}
