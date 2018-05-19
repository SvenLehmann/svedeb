package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import de.hpi.svedeb.table.Column.{ScanColumn, ScannedValues}
import de.hpi.svedeb.table.ColumnType
import org.scalatest.Matchers._

abstract class AbstractActorTest(name: String) extends TestKit(ActorSystem(name))
  with ImplicitSender with AbstractTest {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def checkColumnsValues(column: ActorRef, expectedValues: ColumnType): Unit = {
    column ! ScanColumn()
    val scannedValues = expectMsgType[ScannedValues]
    scannedValues.values shouldEqual expectedValues
  }
}

