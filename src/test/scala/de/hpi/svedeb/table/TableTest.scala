package de.hpi.svedeb.table

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.svedeb.table.Table._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class TableTest extends TestKit(ActorSystem("TableTest")) with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A new table actor" should "store columns" in {
    val table = system.actorOf(Table.props(List("columnA", "columnB"), 10))
    table ! ListColumnsInTable
    expectMsg(ColumnList(List("columnA", "columnB")))
  }

  it should "retrieve partition" in {
    val table = system.actorOf(Table.props(List("columnA"), 10))
    table ! GetPartitions
    expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 }
  }

  ignore should "retrieve column" in {
    val table = system.actorOf(Table.props(List("columnA"), 10))
    table ! GetColumnFromTable("columnA")
    expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 1 }
  }

}
