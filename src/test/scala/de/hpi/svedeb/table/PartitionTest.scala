package de.hpi.svedeb.table

import akka.actor.ActorSystem
import akka.actor.Status.Failure
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import de.hpi.svedeb.table.Partition._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class PartitionTest extends TestKit(ActorSystem("PartitionTest")) with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A partition actor" should "be initialized with columns" in {
    val column = TestProbe("someColumn")
    val partition = system.actorOf(Partition.props(List("someColumn")))

    partition ! ListColumns()
    expectMsgPF() { case m: ColumnList => m.columns.size == 1 }
  }

  it should "return its columns names" in {
    val column = TestProbe("someColumn")
    val partition = system.actorOf(Partition.props(List("someColumn")))

    partition ! ListColumns()
    expectMsgPF() { case m: ColumnList => m.columns.size == 1 }
  }

  it should "return a column ref" in {
    val partition = system.actorOf(Partition.props(List("someColumn")))

    partition ! GetColumn("someColumn")
    expectMsgPF() { case m: RetrievedColumn => m.column.path.toStringWithoutAddress == "someColumn" }
  }

  it should "add a row with one column" in {
    val partition = system.actorOf(Partition.props(List("someColumn")))

    partition ! AddRow(List("someValue"))
    expectMsg(RowAdded())
  }

  it should "add a row with multiple columns" in {
    val partition = system.actorOf(Partition.props(List("column1", "column2")))

    partition ! AddRow(List("value1", "value2"))
    expectMsg(RowAdded())
  }

  ignore should "throw an error when row is added that does not match table columns" in {
    val partition = system.actorOf(Partition.props())
    partition ! AddRow(List("someValue"))
    expectMsg(Failure(new Exception("Wrong number of columns")))
  }


}
