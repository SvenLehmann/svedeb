package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import de.hpi.svedeb.api.API
import de.hpi.svedeb.management.TableManager

object SvedeB {
  def start(): ActorRef = {
    val system = ActorSystem("SvedeB")
    val tableManager = system.actorOf(TableManager.props())
    system.actorOf(API.props(tableManager))
  }
}
