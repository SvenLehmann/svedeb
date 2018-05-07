package de.hpi.svedeb

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

abstract class AbstractActorTest(name: String) extends TestKit(ActorSystem(name))
  with ImplicitSender with AbstractTest {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}

