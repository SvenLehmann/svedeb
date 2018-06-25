package de.hpi.svedeb

import akka.actor.ActorRef
import akka.util.Timeout

import scala.concurrent.duration._
import scala.language.postfixOps

abstract class AbstractBenchmark {
  implicit val timeout: Timeout = Timeout(30 minutes)

  val name: String

  def setup(api: ActorRef, tableSize: Int): Unit

  def runBenchmark(api: ActorRef): Unit

  def tearDown(api: ActorRef): Unit
}
