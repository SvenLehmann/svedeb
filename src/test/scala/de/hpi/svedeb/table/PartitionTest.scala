package de.hpi.svedeb.table

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.svedeb.table.Partition.{AddRowToPartitionMessage, ColumnListMessage, GetColumnsMessage}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Ignore}

class PartitionTest extends TestKit(ActorSystem("PartitionTest")) with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A new partition actor" should "not contain columns" in {
    val partition = system.actorOf(Partition.props())
    partition ! GetColumnsMessage
    expectMsg(ColumnListMessage(List.empty[ActorRef]))
  }

  ignore should "throw an error when rows are added" in {
    val partition = system.actorOf(Partition.props())
    partition ! AddRowToPartitionMessage(List("someValue"))
    expectMsg()
  }
}
