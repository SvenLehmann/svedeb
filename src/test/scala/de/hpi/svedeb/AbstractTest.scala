package de.hpi.svedeb

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

abstract class AbstractTest(name: String) extends TestKit(ActorSystem(name))
  with ImplicitSender with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

}
