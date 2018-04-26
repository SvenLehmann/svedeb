package de.hpi.svedeb.table

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import de.hpi.svedeb.table.Partition._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Ignore}

class PartitionTest extends TestKit(ActorSystem("PartitionTest")) with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An empty partition actor" should "not contain columns" in {
    val partition = system.actorOf(Partition.props())
    partition ! ListColumns
    expectMsg(ColumnList(List.empty[String]))
  }

  ignore should "throw an error when rows are added" in {
    val partition = system.actorOf(Partition.props())
    partition ! AddRow(List("someValue"))
    expectMsg()
  }

  it should "add a column" in {
    val partition = system.actorOf(Partition.props())
    partition ! AddColumn("someColumn")
    expectMsg(ColumnAdded())
  }

  "A partition actor" should "be initialized with columns" in {
    val column = TestProbe()
    val partition = system.actorOf(Partition.props(List(column.ref)))

    partition ! ListColumns
    expectMsgPF() { case m: ColumnList => m.columns.size == 1 }
  }

  it should "add a column" in {
    val column = TestProbe()
    val partition = system.actorOf(Partition.props(List(column.ref)))

    partition ! AddColumn("someColumn")
    expectMsg(ColumnAdded())

    partition ! ListColumns
    expectMsgPF() { case m: ColumnList => m.columns.size == 2 }
  }
}
