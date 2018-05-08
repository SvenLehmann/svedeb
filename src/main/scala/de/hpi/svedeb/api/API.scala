package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

object API {
  def props(tableManager: ActorRef): Props = Props(new API(tableManager))
}

class API(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = ???
}
