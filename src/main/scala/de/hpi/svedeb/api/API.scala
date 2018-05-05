package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, Props}

object API {
  def props(): Props = Props(new API())
}

class API extends Actor with ActorLogging {
  override def receive: Receive = ???
}
