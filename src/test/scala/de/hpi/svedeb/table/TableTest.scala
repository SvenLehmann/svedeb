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
    table ! ListColumnsInTable()
    expectMsg(ColumnList(List("columnA", "columnB")))
  }

  it should "retrieve partition" in {
    val table = system.actorOf(Table.props(List("columnA"), 10))
    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 1 })
  }

  /*ignore should "retrieve columns" in {
    val table = system.actorOf(Table.props(List("columnA"), 10))
    table ! GetColumnFromTable("columnA")
    expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 1 }
  }*/

  it should "add a row" in {
    val table = system.actorOf(Table.props(List("columnA", "columnB"), 10))
    table ! AddRowToTable(List("valueA", "valueB"))
    expectMsg(RowAddedToTable())
  }

  it should "create a new partition if existing ones are full" in {
    print("executing test")
    val table = system.actorOf(Table.props(List("columnA"), 2))
    table ! AddRowToTable(List("value1"))
    table ! AddRowToTable(List("value2"))
    table ! AddRowToTable(List("value3"))

    expectMsg(RowAddedToTable())
    expectMsg(RowAddedToTable())
    expectMsg(RowAddedToTable())

    table ! GetPartitions()
    assert(expectMsgPF() { case m: PartitionsInTable => m.partitions.size == 2 })

    table ! GetColumnFromTable("columnA")
    assert(expectMsgPF() { case m: ActorsForColumn => m.columnActors.size == 2 })
  }

  it should "fail to add wrong row definition" in {
    val table = system.actorOf(Table.props(List("columnA"), 2))
    table ! AddRowToTable(List("value1", "value2"))

  }



}
