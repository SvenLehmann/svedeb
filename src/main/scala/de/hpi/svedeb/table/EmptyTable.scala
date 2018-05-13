package de.hpi.svedeb.table

import akka.actor.{Actor, Props}

object EmptyTable {
  def props(): Props = Props(new EmptyTable())
}

class EmptyTable() extends Actor {
  override def receive: Receive = {
    case _ => // this class doesn't understand any message
  }
}
