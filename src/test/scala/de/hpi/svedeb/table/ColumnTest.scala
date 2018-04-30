package de.hpi.svedeb.table

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.svedeb.table.Column._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class ColumnTest extends TestKit(ActorSystem("ColumnTest")) with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A column actor" should "be empty at start" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! Scan()
    expectMsg(ScannedValues(List.empty[String]))
  }

  it should "insert a new value" in {
    val column = system.actorOf(Column.props("SomeColumnName"))

    column ! AppendValue("SomeValue")
    expectMsg(ValueAppended())
  }

  it should "expose its values" in {
    val column = system.actorOf(Column.props("SomeColumnName"))
    column ! AppendValue("value1")
    column ! AppendValue("value2")
    column ! AppendValue("value3")
    column ! Scan()

    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ValueAppended())
    expectMsg(ScannedValues(List("value1", "value2", "value3")))
  }
}
